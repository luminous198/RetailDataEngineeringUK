import pandas as pd


def add_user_defined_category(productname):
    try:
        productname = productname.lower()
    except Exception as e:
        return None

    if 'milk' in productname:
        return 'Milk'
    return None

if __name__ == "__main__":
    filename = r'C:\Users\milan\Documents\projects\grocery-agg\data\analysis\retail_store_items_full.csv'
    outfilename = r'C:\Users\milan\Documents\projects\grocery-agg\data\analysis\retail_store_items_full_with_category.csv'

    df = pd.read_csv(filename)

    df['computed_category'] = df.apply(lambda x: add_user_defined_category(x.itemname), axis=1)

    df.to_csv(outfilename, index=False)