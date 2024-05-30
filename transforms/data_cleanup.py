import pandas as pd
from commons.configs import (TRANSFORM_DATADIR, UNIFIED_DATAFILE_NAME, CLEANED_DATAFILE_NAME,
                             METADATA_DIR, BRANDMETADATA_FILE)
import os
import numpy as np
import re
from create_product_type import identify_product_type
from utils.file_reading import read_excel_sheet_data, get_excel_sheet_names
from commons.statics import STORE_MORRISSONS, STORE_ALDI, STORE_ASDA


pd.set_option('display.max_columns', None)

def cleanup_price(r):
    if not r or r is np.NaN:
        return
    try:
        r = r.strip()
    except:
        return 'ERROR'
    r = r.replace(u'\xA3', '')
    r = r.replace(u'now', '')
    if 'p' in r:
        r = r.replace('p','')
        r = float(r)
        r = r/100
    r = float(r)
    return r


def standardize_volume_tuple(volume):
    #courtesy of chatgpt, coult be buggy!!!!!!!

    #TODO does not handle package counts eg: 6*25g comes as 0.25kg
    #TODO can be improved further, eg include cm

    """
    Convert volume strings to standardized units and return as a tuple: (standard_unit, value, type).
    """
    if pd.isna(volume):
        return (None, None, None)

    volume = str(volume).lower().strip()

    # Patterns for matching
    patterns = {
        'kg': r'(\d+(?:\.\d+)?)\s*kg',
        'g': r'(\d+(?:\.\d+)?)\s*g',
        'mg': r'(\d+(?:\.\d+)?)\s*mg',
        'l': r'(\d+(?:\.\d+)?)\s*l',
        'ml': r'(\d+(?:\.\d+)?)\s*ml',
        'cl': r'(\d+(?:\.\d+)?)\s*cl',
        'pint': r'(\d+(?:\.\d+)?)\s*pint',
        'pack': r'(\d+)\s*(?:pack|per pack|roll|sheet|pk)',
        'pieces': r'(\d+)\s*(?:pieces|pcs)',
    }

    match = None
    value = None
    unit = None
    type = None

    # Checking for kg first
    if re.search(patterns['kg'], volume):
        match = re.search(patterns['kg'], volume)
        value = float(match.group(1))
        unit = 'kg'
        type = 'weight'
    # Checking for grams
    elif re.search(patterns['g'], volume):
        match = re.search(patterns['g'], volume)
        value = float(match.group(1)) / 1000
        unit = 'kg'
        type = 'weight'
        # Checking for grams
    elif re.search(patterns['mg'], volume):
        match = re.search(patterns['mg'], volume)
        value = float(match.group(1)) / 100
        unit = 'kg'
        type = 'weight'
    # Checking for liters
    elif re.search(patterns['l'], volume):
        match = re.search(patterns['l'], volume)
        value = float(match.group(1))
        unit = 'l'
        type = 'liquid'
    # Checking for milliliters
    elif re.search(patterns['ml'], volume):
        match = re.search(patterns['ml'], volume)
        value = float(match.group(1)) / 1000
        unit = 'l'
        type = 'liquid'
        # Checking for milliliters
    elif re.search(patterns['cl'], volume):
        match = re.search(patterns['cl'], volume)
        value = float(match.group(1)) / 100
        unit = 'l'
        type = 'liquid'
    # Checking for pints
    elif re.search(patterns['pint'], volume):
        match = re.search(patterns['pint'], volume)
        value = float(match.group(1))
        unit = 'pint'
        type = 'liquid'
    # Checking for packs
    elif re.search(patterns['pack'], volume):
        match = re.search(patterns['pack'], volume)
        value = float(match.group(1))
        unit = 'pack'
        type = 'count'
    # Checking for pieces
    elif re.search(patterns['pieces'], volume):
        match = re.search(patterns['pieces'], volume)
        value = float(match.group(1))
        unit = 'pieces'
        type = 'count'

    # If no patterns matched, return None for all fields
    if value is not None and unit is not None and type is not None:
        return (unit, value, type)
    else:
        return (None, None, None)


def clean_product_name(product_name, volume):
    """
    Clean the product name by removing the volume information at the end using the Volume column.
    """
    if isinstance(volume, str) and volume in product_name:
        cleaned_name = product_name.replace(volume, '').strip()
    else:
        cleaned_name = product_name
    return cleaned_name


def identify_brand():
    pass

def cleanup_raw_cols(data):

    data.dropna(subset=['Product Name'], inplace=True)

    #TODO make record serial number - make a serail number for each item for each category in each store
    # will need to account for storename, category, page number, Record Index since we dont have page size
    # Remember, morrissons is a scrollable so page number if always 1 for it

    data['price_cleaned'] = data.apply(lambda x: cleanup_price(x.Price), axis=1)
    data['Standardized_Volume'] = data.apply(lambda x: standardize_volume_tuple(x.Volume), axis=1)

    data.loc[:, 'msr_unit'] = data.Standardized_Volume.map(lambda x: x[0])
    data.loc[:, 'msr_value'] = data.Standardized_Volume.map(lambda x: x[1])
    data.loc[:, 'msr_type'] = data.Standardized_Volume.map(lambda x: x[2])

    data.drop(['Standardized_Volume'], inplace=True, axis=1)

    data['computed_product_type'] = data['Product Name'].apply(
        lambda x: identify_product_type(x))

    data['cleaned_product_name'] = data.apply(lambda x: clean_product_name(x['Product Name'], x['Volume']),
                                              axis=1)

    return data


def make_brand_lookup_data(branddatafile):

    sheetnames = get_excel_sheet_names(branddatafile)
    commons_brands = [x for x in sheetnames if 'top-brands' in x]

    aldi_brands_list = read_excel_sheet_data(branddatafile, 'own-brands-aldi')
    asda_brands_list = read_excel_sheet_data(branddatafile, 'own-brands-asda')
    morrissons_brands_list = read_excel_sheet_data(branddatafile, 'own-brands-morrissons')

    command_brands_list = []
    for _cbrand in commons_brands:
        command_brands_list.extend(read_excel_sheet_data(branddatafile, _cbrand))
    command_brands_list = [x[0].lower() for x in command_brands_list]
    command_brands_list = list(set(command_brands_list))

    data = {
        STORE_ALDI: [x[0].lower() for x in aldi_brands_list],
        STORE_ASDA: [x[0].lower() for x in asda_brands_list],
        STORE_MORRISSONS: [x[0].lower() for x in morrissons_brands_list],
        'common': command_brands_list,
    }
    return data

def add_brand(product_name, storename, category, branddata):

    ret = ([], None, None, None, None)
    try:
        product_name = product_name.lower()
    except:
        return ret

    brand_items = []
    is_morrisson = 0
    is_aldi = 0
    is_asda = 0

    for _store in [STORE_MORRISSONS, STORE_ALDI, STORE_ASDA]:
        if _store != storename:
            continue
        for _brand in branddata[_store]:
            if _brand in product_name:
                brand_items.append(_brand)
                if _store == STORE_MORRISSONS:
                    is_morrisson = 1
                elif _store == STORE_ASDA:
                    is_asda = 1
                elif _store == STORE_ALDI:
                    is_aldi = 1
                else:
                    pass

    for _b in branddata['common']:
        if _b in product_name:
            brand_items.append(_b)

    brand_identified = 1 if len(brand_items) >=1 else 0

    return (brand_items, is_morrisson, is_aldi, is_asda, brand_identified)


if __name__ == "__main__":

    date_to_get = '2024-05-29'
    indatafile = os.path.join(TRANSFORM_DATADIR, UNIFIED_DATAFILE_NAME.format(**{'DATAFILE_DATE': date_to_get}))
    outfilename = os.path.join(TRANSFORM_DATADIR, CLEANED_DATAFILE_NAME.format(**{'DATAFILE_DATE': date_to_get}))

    brandmeta = os.path.join(METADATA_DIR, BRANDMETADATA_FILE)
    branddata = make_brand_lookup_data(brandmeta)

    df = pd.read_csv(indatafile)
    cleaned_df = cleanup_raw_cols(df)

    cleaned_df['brand_data'] = cleaned_df.apply(lambda x: add_brand(x['Product Name'], x['Storename'], x['Category'], branddata),
                                axis=1)

    cleaned_df.loc[:, 'brand_items'] = cleaned_df.brand_data.map(lambda x: x[0])
    cleaned_df.loc[:, 'is_morrisson'] = cleaned_df.brand_data.map(lambda x: x[1])
    cleaned_df.loc[:, 'is_aldi'] = cleaned_df.brand_data.map(lambda x: x[2])
    cleaned_df.loc[:, 'is_asda'] = cleaned_df.brand_data.map(lambda x: x[3])
    cleaned_df.loc[:, 'brand_identified'] = cleaned_df.brand_data.map(lambda x: x[4])

    cleaned_df.drop(['brand_data'], axis=1, inplace=True)

    print(cleaned_df.head())

    cleaned_df.to_csv(outfilename, index=False)


