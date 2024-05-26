from selenium import webdriver
import re
from selenium.webdriver.firefox.options import Options


class BaseScrollableCollector(object):

    def __init__(self):
        pass

    def get_driver(self):
        options = Options()
        options.headless = True
        options.add_argument("--headless")
        return webdriver.Firefox(options=options)