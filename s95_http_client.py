import time
import random
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class S95HttpError(Exception):
    pass


class S95BanDetected(S95HttpError):
    pass


class S95TemporaryError(S95HttpError):
    pass


class S95HttpClient:
    def __init__(
        self,
        base_headers=None,
        connect_timeout=10,
        read_timeout=60,
        min_delay=2.0,
        max_delay=5.0,
        cooldown_seconds=1800,
        max_retries=2,
    ):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.cooldown_seconds = cooldown_seconds
        self.max_retries = max_retries
        self.cooldown_until = 0

        self.base_headers = base_headers or {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept-Encoding": "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://s95.ru/",
        }

        self.session = self._build_session()

    def _build_session(self):
        session = requests.Session()
        session.headers.update(self.base_headers)

        retry = Retry(
            total=0,  # 429/403 не ретраим на уровне urllib3
            connect=0,
            read=0,
            redirect=3,
            status=0,
            backoff_factor=0,
            allowed_methods=frozenset(["GET", "HEAD"]),
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def log(self, message: str):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {message}", flush=True)

    def _sleep_between_requests(self, reason="между запросами"):
        sec = random.uniform(self.min_delay, self.max_delay)
        self.log(f"Сон {sec:.1f}s ({reason})")
        time.sleep(sec)

    def _wait_cooldown_if_needed(self):
        now = time.time()
        if now < self.cooldown_until:
            sleep_for = int(self.cooldown_until - now)
            self.log(f"Cooldown активен, ждём {sleep_for}s")
            time.sleep(sleep_for)

    def _set_cooldown(self, seconds=None, reason="ban-like signal"):
        seconds = seconds or self.cooldown_seconds
        self.cooldown_until = max(self.cooldown_until, time.time() + seconds)
        self.log(f"Установлен cooldown {seconds}s ({reason})")

    def _looks_like_ban_page(self, response: requests.Response) -> bool:
        text_lower = response.text.lower()

        suspicious_markers = [
            "too many requests",
            "access denied",
            "forbidden",
            "temporarily blocked",
            "captcha",
            "cloudflare",
            "ddos",
            "bot check",
        ]

        if response.status_code in (403, 429):
            return True

        if len(response.text.strip()) < 500:
            return True

        return any(marker in text_lower for marker in suspicious_markers)

    def get_text(self, url: str, allow_ban_html_check=True, sleep_before=True, sleep_after=False) -> str:
        self._wait_cooldown_if_needed()

        if sleep_before:
            self._sleep_between_requests("перед запросом")

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    timeout=(self.connect_timeout, self.read_timeout),
                )

                if response.status_code in (403, 429):
                    self._set_cooldown(reason=f"http {response.status_code}")
                    raise S95BanDetected(f"HTTP {response.status_code} for {url}")

                response.raise_for_status()

                if allow_ban_html_check and self._looks_like_ban_page(response):
                    self._set_cooldown(reason="suspicious html")
                    raise S95BanDetected(f"Suspicious HTML for {url}")

                if sleep_after:
                    self._sleep_between_requests("после успешного запроса")

                return response.text

            except S95BanDetected:
                raise

            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                last_error = e
                self.log(f"Сетевая ошибка attempt={attempt}/{self.max_retries} для {url}: {e}")
                if attempt < self.max_retries:
                    time.sleep(random.uniform(5, 15))
                    continue
                raise S95TemporaryError(f"Network error for {url}: {e}") from e

            except requests.exceptions.RequestException as e:
                last_error = e
                raise S95HttpError(f"HTTP error for {url}: {e}") from e

        raise S95HttpError(f"Unknown error for {url}: {last_error}")

    def get_soup(self, url: str, allow_ban_html_check=True, sleep_before=True, sleep_after=False) -> BeautifulSoup:
        html = self.get_text(
            url,
            allow_ban_html_check=allow_ban_html_check,
            sleep_before=sleep_before,
            sleep_after=sleep_after,
        )
        return BeautifulSoup(html, "html.parser")

    def reset_session(self):
        self.log("🔄 Пересоздаю HTTP session")

        try:
            self.session.close()
        except Exception:
            pass

        self.session = self._build_session()