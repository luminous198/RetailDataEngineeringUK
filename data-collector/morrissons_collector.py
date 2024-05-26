from selenium.webdriver.common.by import By
from urllib.error import ContentTooShortError
import time
import pandas as pd
import os
import datetime
from random import randint
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib3.exceptions import NewConnectionError, ConnectionError, MaxRetryError
from base_scrollable_collector import BaseScrollableCollector
from commons.configs import DATADIR_PATH
import re



class MorrissonsCollector(BaseScrollableCollector):

    def __init__(self):
        super(MorrissonsCollector, self).__init__()
        self.output_datadir = os.path.join(DATADIR_PATH, 'MORRISSONS')
        try:
            os.makedirs(self.output_datadir)
        except:
            pass
        self.sleep_time = 10
        self.base_url = 'https://groceries.morrisons.com/browse/'
        self.css_elements_map = {
            'product_name': ('#main-content .fops-item div.fop-description > h4'),
            'product_href': ('#main-content div.fop-contentWrapper'),
            'product_price': ('#main-content .fops-item span.fop-price'),
            'product_price_per_kg': ('#main-content .fops-item span.fop-unit-price'),
            'product_boxes_data': '#main-content .fops-item'
        }


        self.categories = ['meat-poultry-179549', 'fruit-veg-176738', 'fresh-176739', 'fish-seafood-184367',
                       'bakery-cakes-102210', 'food-cupboard-102705', 'chocolate-sweets-106130', 'frozen-180331',
                       'toiletries-beauty-102838', 'drinks-103644', 'beer-wines-spirits-103120', 'household-102063',
                       'home-garden-166274', 'health-wellbeing-medicines-103497', 'baby-toddler-177598',
                       'toys-entertainment-166275',
                       'pet-shop-102207', 'free-from-175652', 'world-foods-182137']

    def scrape(self):

        start_time = time.time()
        out_file_time = str(datetime.datetime.now())
        out_file_time = out_file_time.replace(' ', '').replace('.','').replace(':','')
        output_file = os.path.join(self.output_datadir, 'raw_data_{0}'.format(out_file_time + '.csv'))

        dataset = []

        for _category in self.categories:

            print(f'started category: {_category}')
            driver = self.get_driver()
            driver.set_script_timeout(30)
            boxes_rect = []

            url_to_fetch = f'{self.base_url}/{_category}?display=10000&showOOS=true'

            driver.get(url_to_fetch)
            time.sleep(randint(7, 10))

            boxes = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_boxes_data'])
            boxes_rect.extend([box.rect for box in boxes])

            i = 0
            try:

                while i<len(boxes):
                    scroll_position = boxes[i].rect['y']
                    driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    i += 50
                    time.sleep(2)

                raw_names = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_name'])
                product_hrefs = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_href'])
                prices = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_price'])
                price_per_kg = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_price_per_kg'])

                price_regex = re.compile(r'[^\d.,]+')


                names = map(lambda x: x.text, raw_names)
                names_hrefs = map(lambda x: x.get_attribute('href'), product_hrefs)
                prices = map(lambda x: x.text, prices)
                prices = map(lambda x: price_regex.sub('', x), prices)
                prices = map(lambda x: x.replace(u'\xA3', ''), prices)
                price_per_kg = map(lambda x: x.text, price_per_kg)
                price_per_kg = map(lambda x: x.replace(u'\xA3', ''), price_per_kg)

                category_list = [_category] * len(raw_names)

                dummy_page_number = [1] * len(raw_names)
                page_product_data = list(zip(dummy_page_number, names, prices, price_per_kg, names_hrefs, category_list))
                dataset.extend(page_product_data)

            except (TimeoutException, WebDriverException, ConnectionError, NewConnectionError, MaxRetryError, TimeoutError, ContentTooShortError):
                print(f'Request error for category: {_category}')
                scroll_position = boxes[i].rect['y']  # Store the scroll position before quitting
                driver.quit()
                time.sleep(30)
                driver = self.get_driver()
                driver.get(url_to_fetch)
                driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                continue

            driver.quit()
        df = pd.DataFrame(dataset, columns =['Page Number', 'Product Name', 'Price',
                                             'Price_per_KG', 'Product HREF', 'Category'])
        df.to_csv(output_file, index=False)

        end_time = time.time()

        total_time = end_time - start_time
        print('Total time taken', total_time)

if __name__ == "__main__":
    obj = MorrissonsCollector()
    obj.scrape()