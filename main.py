import requests, re
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
import warnings
import time
import re
from datetime import datetime

