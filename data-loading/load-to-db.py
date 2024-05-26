from commons.configs import DATADIR_PATH
import os
import pandas as pd
from utils.file_reading import read_folder_and_get_union
from dbsetup import Session, engine
from datetime import datetime


pd.set_option('display.max_columns', None)

def get_asda_dataset(datadir, data_date):
    additional_cols = [{'colname': 'scrape_date', 'colvalue': data_date}]
    asda_path = os.path.join(os.path.join(datadir, 'ASDA'), data_date)
    asda_data = read_folder_and_get_union(asda_path, staic_col_params=additional_cols)
    asda_data['Storename'] = 'ASDA'
    return asda_data


def get_aldi_dataset(datadir, data_date):
    additional_cols = [{'colname': 'scrape_date', 'colvalue': data_date}]
    aldi_path = os.path.join(os.path.join(datadir, 'ALDI'), data_date)
    aldi_data = read_folder_and_get_union(aldi_path, staic_col_params=additional_cols)
    aldi_data['Storename'] = 'ALDI'
    return aldi_data


def get_morrissons_dataset(datadir, data_date):
    morr_path = os.path.join(os.path.join(datadir, 'MORRISSONS'), data_date)
    morr_datafile = os.listdir(morr_path)[0]
    morr_datafilepath = os.path.join(morr_path, morr_datafile)
    df = pd.read_csv(morr_datafilepath, sep=',')
    df['scrape_date'] = data_date
    df['Storename'] = 'MORRISSONS'
    return df


def load_data(datadf):
    print(datadf.head())

    datadf['create_datetime'] = datetime.now()
    with Session() as session:
        datadf.to_sql('retail_store_items', con=engine, if_exists='append', index=False,
                      schema='in_use')

if __name__ == "__main__":

    date_to_get = '2024-05-25'

    asda_data = get_asda_dataset(DATADIR_PATH, date_to_get)
    aldi_data = get_aldi_dataset(DATADIR_PATH, date_to_get)
    morrissons_data = get_morrissons_dataset(DATADIR_PATH, date_to_get)

    combined_df = pd.concat([asda_data,  aldi_data, morrissons_data], ignore_index=True)
    combined_df.columns = ['item_page_number', 'itemname', 'price', 'price_per_unit',
                           'item_link', 'item_category',  'scrape_date', 'storename']
    print(asda_data.columns)
    print(aldi_data.columns)
    print(morrissons_data.columns)

    print(combined_df.columns)
    # print(combined_df.head())

    load_data(combined_df)
