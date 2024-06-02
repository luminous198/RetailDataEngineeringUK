from selenium import webdriver
import re
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options as ChromeOptions
from commons.configs import SELENIUM_DRIVER_TYPE, CHROME_REMOTE_PORT


class BaseScrollableCollector(object):

    def __init__(self):
        pass
    
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