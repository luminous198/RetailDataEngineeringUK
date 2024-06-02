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
from commons.configs import SELENIUM_DRIVER_TYPE, CHROME_REMOTE_PORT



class BaseCollector(object):

    def __init__(self):
        pass

    # def get_driver(self):
    #     options = Options()
    #     options.headless = True
    #     options.set_preference("dom.popup_maximum", 2)
    #     options.add_argument("--headless")

    #     return webdriver.Firefox(options=options)
    
    # def get_driver(self):
    #     options = Options()
    #     options.headless = True
    #     options.add_argument("--headless")
    #     remote_webdriver = 'remote_chromedriver'
    #     return webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options)
    
    # def get_driver(self):
    #     options = FirefoxOptions()
    #     cloud_options = {}
    #     cloud_options['build'] = "build_1"
    #     cloud_options['name'] = "test_abc"
    #     options.set_capability('cloud:options', cloud_options)
    #     return webdriver.Remote(
    #       command_executor='http://host.docker.internal:4444/wd/hub'
    #       , options=options) 
    
    def get_driver(self):
        if SELENIUM_DRIVER_TYPE == 'chrome':
            options = ChromeOptions()
            return webdriver.Remote(
                command_executor='http://chrome:{0}/wd/hub'.format(CHROME_REMOTE_PORT)
                , options=options)
        else:
            options = Options()
            options.headless = True
            options.set_preference("dom.popup_maximum", 2)
            options.add_argument("--headless")

            return webdriver.Firefox(options=options)



