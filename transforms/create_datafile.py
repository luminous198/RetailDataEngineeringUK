from commons.configs import DATADIR_PATH
from commons.configs import TRANSFORM_DATADIR, UNIFIED_DATAFILE_NAME
import os
import pandas as pd
from commons.statics import STORE_MORRISSONS, STORE_ALDI, STORE_ASDA
from data_utils.file_reading import read_folder_and_get_union
from datetime import datetime


pd.set_option('display.max_columns', None)

def get_asda_dataset(datadir, data_date):
    additional_cols = [{'colname': 'scrape_date', 'colvalue': data_date}]
    asda_path = os.path.join(os.path.join(datadir, STORE_ASDA), data_date)
    asda_data = read_folder_and_get_union(asda_path, staic_col_params=additional_cols)
    asda_data['Storename'] = STORE_ASDA
    return asda_data


def get_aldi_dataset(datadir, data_date):
    additional_cols = [{'colname': 'scrape_date', 'colvalue': data_date}]
    aldi_path = os.path.join(os.path.join(datadir, STORE_ALDI), data_date)
    aldi_data = read_folder_and_get_union(aldi_path, staic_col_params=additional_cols)
    aldi_data['Storename'] = STORE_ALDI
    return aldi_data


def get_morrissons_dataset(datadir, data_date):
    additional_cols = [{'colname': 'scrape_date', 'colvalue': data_date}]
    morrissons_path = os.path.join(os.path.join(datadir, STORE_MORRISSONS), data_date)
    morrissons_data = read_folder_and_get_union(morrissons_path, staic_col_params=additional_cols)
    morrissons_data['Storename'] = STORE_MORRISSONS
    return morrissons_data


def unify_data(asda_data, aldi_data, morrissons_data):

    if 'Price_per_KG' in morrissons_data.columns:
        morrissons_data.rename(columns={'Price_per_KG': 'Price per UOM'}, inplace=True)

    morrissons_data.rename(columns={'Weight': 'Volume'}, inplace=True)
    morrissons_data.rename(columns={'rating': 'Rating'}, inplace=True)
    aldi_data.rename(columns={'Pack Size': 'Volume'}, inplace=True)

    aldi_data['Rating'] = ''

    aldi_data['promotion'] = ''
    asda_data['offer'] = ''

    asda_data['promotion'] = ''
    aldi_data['offer'] = ''

    aldi_data['Review Count'] = ''
    morrissons_data['Review Count'] = ''

    common_cols = ['Product Name', 'Product HREF', 'Category', 'Page Number', 'Record Index',
       'scrape_date', 'Storename', 'Price', 'Price per UOM', 'Volume', 'Rating', 'promotion',
                   'offer', 'Review Count']

    aldi_df = aldi_data[common_cols]
    morrissons_df = morrissons_data[common_cols]
    asda_df = asda_data[common_cols]

    combined_df = pd.concat([aldi_df, morrissons_df, asda_df], ignore_index=True)
    return combined_df


def create_datafile(date_to_get):
    outfilename = os.path.join(TRANSFORM_DATADIR, UNIFIED_DATAFILE_NAME.format(**{'DATAFILE_DATE': date_to_get}))

    asda_data = get_asda_dataset(DATADIR_PATH, date_to_get)
    aldi_data = get_aldi_dataset(DATADIR_PATH, date_to_get)
    morrissons_data = get_morrissons_dataset(DATADIR_PATH, date_to_get)
    combined_data = unify_data(asda_data, aldi_data, morrissons_data)
    combined_data.to_csv(outfilename, index=False)

