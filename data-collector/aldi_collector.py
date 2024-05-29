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
from commons.configs import DATADIR_PATH, MAX_PAGE_PER_CATEGORY
from base_collector import BaseCollector
from bs4 import BeautifulSoup




class ALDICollector(BaseCollector):

    def __int__(self):
        super(ALDICollector, self).__init__()
        self.output_datadir = os.path.join(os.path.join(DATADIR_PATH, 'ALDI'), str(datetime.datetime.now().date()))
        try:
            os.makedirs(self.output_datadir)
        except:
            pass
        self.sleep_time = 10
        self.categories = ['vegan-range', 'bakery', 'fresh-food', 'drinks', 'food-cupboard',
                       'frozen', 'chilled-food', 'baby-toddler', 'health-beauty',
                           'household', 'pet-care']
        self.base_url = 'https://groceries.aldi.co.uk/en-GB'
        self.max_page_limit_per_category = MAX_PAGE_PER_CATEGORY
        self.css_elements_map = {
            'last_page_number': 'ul .d-flex-inline.pt-2',
            'product_name': '#vueSearchResults .p.text-default-font',
            'product_price': '#vueSearchResults div.product-tile-price.text-center > div > span > span',
            'product_price_per_kg': '#vueSearchResults div.product-tile-price.text-center > div > div > p > small > span',
        }

    def extract_product_data(self, boxhtml):
        soup = BeautifulSoup(boxhtml, 'html.parser')

        product = {}
        extraction_attrs = [
            ('Product ID', 'div', 'product-tile', 'data-product-id'),
            ('Product HREF', 'a', 'product-tile-media', 'href'),
            ('Image URL', 'img', 'product-image', 'src'),
            ('Product Name', 'a', 'p text-default-font', 'text'),
            ('Pack Size', 'div', 'text-gray-small', 'text'),
            ('Price', 'span', 'h4', 'text'),
            ('Price per UOM', 'small', 'text-gray-small', 'text')
        ]

        for _attr in extraction_attrs:
            try:
                elem = soup.find(_attr[1], class_=_attr[2])
                if _attr[3] == 'text':
                    product[_attr[0]] = elem.get_text(strip=True) if elem else None
                else:
                    product[_attr[0]] = elem[_attr[3]] if elem and _attr[3] in elem.attrs else None
            except Exception as e:
                product[_attr[0]] = f'ERROR: {str(e)}'

        return product

    def scrape(self):

        out_file_time = str(datetime.datetime.now())
        out_file_time = out_file_time.replace(' ', '').replace('.','').replace(':','')

        outcols = ['Product ID', 'Product HREF', 'Image URL', 'Product Name', 'Pack Size', 'Price', 'Price per UOM',
                   'Category', 'Page Number', 'Record Index']

        for _category in self.categories:
            dataset = []
            output_file = os.path.join(self.output_datadir,
                                       'raw_data_{0}-{1}-{2}'.format(out_file_time, _category, '.csv'))
            print('Working on Category {0}'.format(_category))
            last_page_number = self._get_last_page_number(f'{self.base_url}/{_category}')

            print('Found pages for category {0} -> {1}'.format(_category, last_page_number))

            page = 1
            while page <= last_page_number:

                print('Running Page Number {0} for category {1}'.format(page, _category))

                driver = self.get_driver()

                try:
                    driver.get(f'{self.base_url}/{_category}?&page={page}')
                    time.sleep(randint(2, 5))

                    boxes = driver.find_elements(By.CSS_SELECTOR, "div.product-tile")

                    for ind, _box in enumerate(boxes):
                        _boxhtml = _box.get_attribute('innerHTML')
                        boxjson = self.extract_product_data(_boxhtml)
                        boxjson['Category'] = _category
                        boxjson['Page Number'] = page
                        boxjson['Record Index'] = ind
                        dataset.append(boxjson)

                    page +=1
                except (TimeoutException, WebDriverException,
                        ConnectionError, NewConnectionError, MaxRetryError, TimeoutError,
                        ContentTooShortError) as exc:
                    driver.quit()
                    print('Found error in page {0}', page)
                    #Page could not be fetched, restart driver again
                    # driver = self.get_driver()
                    continue

                driver.quit()

            df = pd.DataFrame(dataset, columns =outcols)
            df.to_csv(output_file, index=False)

    def _get_last_page_number(self, url_to_fetch):

        driver = self.get_driver()
        driver.get(url_to_fetch)
        time.sleep(self.sleep_time)
        last_page_css = self.css_elements_map['last_page_number']
        try:
            last_page_number = driver.find_element(By.CSS_SELECTOR, last_page_css).text
            last_page_number = last_page_number.replace('of', '').replace(' ', '')
            last_page_number = int(last_page_number)
        except NoSuchElementException:
            last_page_number = 1
            driver.quit()

        except (TimeoutException, WebDriverException, ValueError,
                ConnectionError, NewConnectionError, MaxRetryError, TimeoutError, ContentTooShortError):
            driver.quit()
            #try again with a timeout
            driver1 = self.get_driver()
            driver1.get(f'{self.base_url}/search/{url_to_fetch}')
            wait = WebDriverWait(driver1, 30)  # Wait for a maximum of 30 seconds
            time.sleep(self.sleep_time)
            last_page_number = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, last_page_css))).text
            last_page_number = last_page_number.replace('of', '').replace(' ', '')
            last_page_number = int(last_page_number)
            driver1.quit()

        if self.max_page_limit_per_category and int(last_page_number) > self.max_page_limit_per_category:
            return self.max_page_limit_per_category
        driver.quit()
        return int(last_page_number)

if __name__ == "__main__":
    x = ALDICollector()
    x.__int__()
    x.scrape()