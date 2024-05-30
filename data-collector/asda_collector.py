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




class ASDACollector(BaseCollector):

    def __int__(self):
        super(ASDACollector, self).__init__()
        self.output_datadir = os.path.join(os.path.join(DATADIR_PATH, 'ASDA'), str(datetime.datetime.now().date()))
        try:
            os.makedirs(self.output_datadir)
        except:
            pass
        self.sleep_time = 10
        self.categories = ['vegan-vegetarian', 'dietary-lifestyle',
         'fruit', 'vegetables-potatoes', 'salads-stir-fry', 'extra-special-fruit-veg', 'raw-nuts-seeds-dried-fruit',
         'meat-poultry', 'fish-seafood',
         'cooked-meat',
         'beer', 'wine', 'spirits', 'cider',
         'bakery','fizzy-drinks',
         # chilled-food
         'milk-butter-cream-eggs', 'cheese', 'yogurts-desserts',
         'chilled-juice-smoothies', 'ready-meals',
                           'pizza-pasta-garlic-bread',
         'party-food-pies-salads-dips', 'sandwiches',
         'frozen-food',
         # Food-cupboard
         'cereals-cereal-bars', 'chocolates-sweets', 'crisps-nuts-popcorn',
         'biscuits-crackers', 'tinned-food',
         'condiments-cooking-ingredients', 'cooking-sauces-meal-kits-sides', 'rice-pasta-noodles',
         'coffee-tea-hot-chocolate',
         'jams-spreads-desserts', 'noodle-pots-instant-snacks', 'home-baking', 'under-100-calories', 'world-food',
         # drinks
         'squash-cordial', 'water', 'tonic-water-mixers', 'fruit-juice', 'sports-energy-drinks',
         'coffee-tea-hot-chocolate',
         # beer wine spirits
         'prosecco-champagne', 'pre-mixed-cocktails',
         'tobacconist',
         # 'toiletries-beauty',
         "hair-care-dye-styling", "make-up-nails", "mens-toiletries", "womens-toiletries", "oral-dental-care",
         "period-products",
         "skin-care", "bath-shower-soap", 'beauty-electricals', 'sunscreen', 'bladder-weakness',
         'health-wellness', 'sun-care-tanning', "toiletries-accessories",
         "air-fresheners", "batteries", "bin-bags", "cleaning", "dishwasher", "fabric-conditioners",
         "household-accessories", "ironing",
         "kitchen-roll", "laundry", "light-bulbs", "shoe-care", "toilet-roll", "washing-powder-liquid",
         'baby-toddler-kids',
         'pets',
         # Home
         'bed-bath-home', 'kitchen', 'music-films-books', "dvds-blu-rays", "gaming", "headphones-speakers",
         "smart-home",
         "tvs-accessories", 'diy-car-care', 'toys', 'technology-electricals', 'flowers'
         ]
        self.base_url = 'https://groceries.asda.com'
        self.max_page_limit_per_category = MAX_PAGE_PER_CATEGORY
        self.css_elements_map = {
            'last_page_number': '#main-content div.co-pagination__max-page > a',
            'product_name': ('#main-content '
                            '.co-product-list__main-cntr.co-product-list__main-cntr--rest-in-shelf '
                            '.co-product__anchor'),
            'product_price': ('#main-content '
                             '.co-product-list__main-cntr.co-product-list__main-cntr--rest-in-shelf '
                             '.co-product__price'),
            'product_price_per_kg': ('#main-content '
                                    '.co-product-list__main-cntr.co-product-list__main-cntr--rest-in-shelf '
                                    '.co-product__price-per-uom'),
        }

    def extract_product_data(self, boxhtml):
        soup = BeautifulSoup(boxhtml, 'html.parser')

        product = {}
        extraction_attrs = [
            ('Product Name', 'h3', 'co-product__title', 'text'),
            ('Product HREF', 'a', 'co-product__anchor', 'href'),
            ('Image URL', 'img', 'asda-img', 'src'),
            ('Volume', 'span', 'co-product__volume', 'text'),
            ('Rating', 'button', 'co-product__rating', 'aria-label'),
            ('Review Count', 'span', 'co-product__review-count', 'text'),
            ('Price', 'strong', 'co-product__price', 'text'),
            ('Price_per_UOM', 'span', 'co-product__price-per-uom', 'text'),
            ('Promotion Icon Alt', 'img', 'co-product__promo-icon-img', 'alt'),
            ('Promotion Icon Title', 'img', 'co-product__promo-icon-img', 'title')
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

        out_file_time = str(datetime.datetime.now())
        out_file_time = out_file_time.replace(' ', '').replace('.','').replace(':','')

        outcols = ['Product Name', 'Product HREF', 'Image URL', 'Volume', 'Rating', 'Review Count', 'Price',
                   'Price per UOM', 'Promotion Icon Alt', 'Promotion Icon Title', 'Category',
                   'Page Number', 'Record Index']

        for _category in self.categories:
            dataset = []
            output_file = os.path.join(self.output_datadir, 'raw_data_{0}-{1}-{2}'.format(out_file_time, _category, '.csv'))
            print('Working on Category {0}'.format(_category))
            last_page_number = self._get_last_page_number(f'{self.base_url}/search/{_category}')

            print('Found pages for category {0} -> {1}'.format(_category, last_page_number))

            page = 1
            while page <= last_page_number:

                print('Running Page Number {0} for category {1}'.format(page, _category))

                driver = self.get_driver()

                try:
                    driver.get(f'{self.base_url}/search/{_category}/products?page={page}')
                    time.sleep(randint(2, 5))

                    boxes = driver.find_elements(By.CSS_SELECTOR, ".co-product")

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
            df = pd.DataFrame(dataset, columns=outcols)
            df.to_csv(output_file, index=False)



        end_time = time.time()

        total_time = end_time - start_time
        print('Total time', total_time)

    def _get_last_page_number(self, url_to_fetch):

        driver = self.get_driver()
        driver.get(url_to_fetch)
        time.sleep(self.sleep_time)
        last_page_css = self.css_elements_map['last_page_number']
        try:
            last_page_number = driver.find_element(By.CSS_SELECTOR, last_page_css).text
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
            driver1.quit()

        if self.max_page_limit_per_category and int(last_page_number) > self.max_page_limit_per_category:
            driver.quit()
            return self.max_page_limit_per_category
        driver.quit()
        return int(last_page_number)

if __name__ == "__main__":
    x = ASDACollector()
    x.__int__()
    x.scrape()