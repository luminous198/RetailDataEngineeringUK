import pandas as pd
import os


def read_folder_and_get_union(datadir, filesep=',', staic_col_params=[]):
    '''

    Args:
        datadir:
        filesep:
        staic_col_params:

    Returns:
    staic_col_params : [
        {
            'colname': 'date',
            'colvalue': '2022-01-01'
        }

    ]
    '''
    datafiles = os.listdir(datadir)

    datadfs = []
    for _file in datafiles:
        _path = os.path.join(datadir, _file)
        _df = pd.read_csv(_path, sep=filesep)
        datadfs.append(_df)

    if len(datadfs) == 0:
        return None

    df_merged = pd.concat(datadfs, ignore_index=True)

    for _item in staic_col_params:
        df_merged[_item['colname']] = _item['colvalue']

    return df_merged
