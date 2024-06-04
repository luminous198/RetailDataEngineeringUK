from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.error import ContentTooShortError
from urllib3.exceptions import NewConnectionError, ConnectionError, MaxRetryError
import time
import pandas as pd
import os
import datetime
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.common.exceptions import NoSuchElementException
from random import randint
from selenium import webdriver
import re
from selenium.webdriver.firefox.options import Options
from commons.configs import DATADIR_PATH
from selenium.webdriver.chrome.options import Options as ChromeOptions
from commons.configs import SELENIUM_DRIVER_TYPE, REMOTE_PORT
from selenium.webdriver.firefox.options import Options as FirefoxOptions




class BaseCollector(object):

    def __init__(self):
        pass

    def make_category_list(self, all_categories):
        #check if category has already been extracted.
        categories_to_skip = []
        present_files = os.listdir(self.output_datadir)
        for _category in all_categories:
            for _present_file in present_files:
                if _category in _present_file:
                    categories_to_skip.append(_category)
        
        categories_to_keep = [x for x in all_categories if x not in categories_to_skip]
        return categories_to_keep

    def get_driver(self):
        if SELENIUM_DRIVER_TYPE == 'chrome':
            options = ChromeOptions()
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--headless')
            return webdriver.Remote(
                command_executor='http://chrome:{0}/wd/hub'.format(REMOTE_PORT)
                , options=options)
        elif SELENIUM_DRIVER_TYPE == 'firefox' and REMOTE_PORT:
            options = FirefoxOptions()
            options.add_argument("--headless")
            return webdriver.Remote(
                command_executor='http://firefox:{0}/wd/hub'.format(REMOTE_PORT)
                , options=options)
        else:
            options = Options()
            options.headless = True
            options.set_preference("dom.popup_maximum", 2)
            options.add_argument("--headless")

            return webdriver.Firefox(options=options)



