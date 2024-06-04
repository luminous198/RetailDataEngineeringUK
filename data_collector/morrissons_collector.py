from selenium.webdriver.common.by import By
from urllib.error import ContentTooShortError
import time
import pandas as pd
import os
import datetime
from random import randint
from selenium.common.exceptions import TimeoutException, WebDriverException
from urllib3.exceptions import NewConnectionError, ConnectionError, MaxRetryError
from data_collector.base_scrollable_collector import BaseScrollableCollector
from commons.configs import DATADIR_PATH
import re
from bs4 import BeautifulSoup
from commons.configs import MAX_CATEGORIES
from static_vars.statics import STORE_MORRISSONS



class MorrissonsCollector(BaseScrollableCollector):

    def __init__(self):
        super(MorrissonsCollector, self).__init__()
        self.output_datadir = os.path.join(os.path.join(DATADIR_PATH, STORE_MORRISSONS), str(datetime.datetime.now().date()))
        try:
            os.makedirs(self.output_datadir)
        except:
            pass
        self.sleep_time = 10
        self.base_url = 'https://groceries.morrisons.com/browse/'
        self.css_elements_map = {
            'product_boxes_data': '#main-content .fops-item',
            'product_tiles': 'li.fops-item'
        }
        all_categories = ['meat-poultry-179549', 'fruit-veg-176738', 'fresh-176739', 'fish-seafood-184367',
                       'bakery-cakes-102210', 'food-cupboard-102705', 'chocolate-sweets-106130', 'frozen-180331',
                       'toiletries-beauty-102838', 'drinks-103644', 'beer-wines-spirits-103120', 'household-102063',
                       'home-garden-166274', 'health-wellbeing-medicines-103497', 'baby-toddler-177598',
                       'toys-entertainment-166275',
                       'pet-shop-102207', 'free-from-175652', 'world-foods-182137']
        self.categories = self.make_category_list(all_categories)
        if MAX_CATEGORIES and len(self.categories) > MAX_CATEGORIES:
            self.categories = self.categories[:MAX_CATEGORIES]
    
    def box_html_to_json(self, boxhtml):
        soup = BeautifulSoup(boxhtml, 'html.parser')

        product = {}
        extraction_attrs = [
            ('offer', 'span', 'fop-ribbon__title', 'text'),
            ('Product Name', 'h4', 'fop-title', 'text'),
            ('sku', 'div', 'fop-item', 'data-sku'),
            ('Product HREF', 'a', 'fop-contentWrapper', 'href'),
            ('image_url', 'img', 'fop-img', 'src'),
            ('product_life', 'span', 'fop-life', 'text'),
            ('Price', 'span', 'fop-price', 'text'),
            ('Price per UOM', 'span', 'fop-unit-price', 'text'),
            ('rating', 'span', 'fop-rating-inner', 'title'),
            ('promotion', 'a', 'fop-row-promo', 'text'),
            ('Weight', 'span', 'fop-catch-weight-inline', 'text')
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

        start_time = time.time()
        out_file_time = str(datetime.datetime.now().date())
        out_file_time = out_file_time.replace(' ', '').replace('.','').replace(':','')

        outcols = ['offer', 'Product Name', 'sku', 'Product HREF', 'product_life', 'Price', 'Price per UOM',
                   'rating', 'promotion', 'Category', 'Page Number', 'Record Index', 'Weight']

        for _category in self.categories:
            output_file = os.path.join(self.output_datadir,
                                       'raw_data_{0}-{1}-{2}'.format(out_file_time, _category, '.csv'))
            dataset = []

            print(f'started category: {_category}')
            driver = self.get_driver()
            driver.set_script_timeout(30)

            url_to_fetch = f'{self.base_url}/{_category}?display=10000&showOOS=true'
            driver.get(url_to_fetch)
            time.sleep(randint(7, 10))


            scrollbox = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_boxes_data'])
            scrollboxes_rect = []
            scrollboxes_rect.extend([box.rect for box in scrollbox])
            j = 0
            try:
                while j<len(scrollboxes_rect):
                    scroll_position = scrollboxes_rect[j]['y']
                    driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                    j += 50
                    time.sleep(2)
            except Exception as e:
                scroll_position = scrollboxes_rect[j]['y']
                driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                driver.quit()
                driver = self.get_driver()

            try:
                boxes = driver.find_elements(By.CSS_SELECTOR, self.css_elements_map['product_tiles'])
                for ind, _box in enumerate(boxes):
                    _boxdata = _box.get_attribute('innerHTML')
                    boxjson = self.box_html_to_json(_boxdata)
                    boxjson['Category'] = _category
                    boxjson['Page Number'] = 1
                    boxjson['Record Index'] = ind
                    dataset.append(boxjson)

            except (TimeoutException, WebDriverException, ConnectionError, NewConnectionError, MaxRetryError, TimeoutError, ContentTooShortError):
                print(f'Request error for category: {_category}')
                driver.quit()
                time.sleep(30)
                driver = self.get_driver()
                driver.get(url_to_fetch)
                continue

            driver.quit()
            df = pd.DataFrame(dataset, columns =outcols)
            df.to_csv(output_file, index=False)

        end_time = time.time()

        total_time = end_time - start_time
        print('Total time taken', total_time)


def scrape_morrissons():
    obj = MorrissonsCollector()
    obj.scrape()

if __name__ == "__main__":
    scrape_morrissons()