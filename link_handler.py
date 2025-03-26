import re


def main_link_event(link):
    '''На вход подаем ссылку на любую страницу внутри парка, возвращается главная страница парка'''
    pattern = r'^https?://[^/]+/([^/]+)/'
    match = re.search(pattern, link)
    if match:
        return match.group(0)
    return None


def link_about_event(main_link):
    '''На вход подаем главную страницу парка, получаем ссылку на страницу о парке'''
    return main_link + 'course/'


def link_latest_result_event(main_link):
    '''На вход подаем главную страницу парка, получаем ссылку на страницу с последней пробежкой'''
    return main_link + 'results/latest/'


def link_all_result_event(main_link):
    '''На вход подаем главную страницу парка, получаем ссылку на страницу со всеми протоколами'''
    return main_link + 'results/all/'


def link_protocol_from_date(main_link, date_event):
    '''На вход подаем главную страницу парка и дату пробежки, вернется ссылка на результаты забега'''
    formatted_date = date_event.strftime("%d.%m.%Y")
    return f'{main_link}results/{formatted_date}/'
