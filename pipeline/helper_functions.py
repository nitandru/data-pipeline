import pandas as pd
import logging
import os
from typing import Any
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def read_data(file_path: str) -> pd.DataFrame:
    """Read a file into a Pandas dataframe based on its extension type
    Credits to `https://gist.github.com/rfeers`
    
    Args:
        file_path (str): Path file to data

    Returns:
        _type_: pd.DataFrame
    """
    _, file_ext = os.path.splitext(file_path)
    if file_ext == 'csv':
        return pd.read_csv(file_path)
    elif file_ext == '.json':
        return pd.read_json(file_path)
    elif file_ext in ['.xls', '.xlsx']:
        return pd.read_excel(file_path)
    else:
        return logging.error(f'Format {file_ext} not recognized')
    

def explore_data(dataframe: pd.DataFrame, pk_columns: list) -> Any:
    """Shows general information about a dataframe

    Args:
        dataframe (pd.DataFrame): The dataframe read
        pk_columns (list): columns choosen to be primary keys over the SQL table.
    """
    
    #Show shape
    shape = dataframe.shape
    logging.info(f'Dataframe has {shape[0]} rows and {shape[1]} columns')
    
    #1. Check missings
    nan_count = dataframe.isna().sum().to_dict()
    for k, v in nan_count.items():
        logging.warning(f'Column {k} has {v} ({v/shape[0]:.2f}%) missing observations')
    
    #2. Check missing and duplicates in pk_columns
    for col in pk_columns:
        df_col_miss = dataframe[col].isna().sum()
        df_col_dups = dataframe[col].duplicated().sum()
        if df_col_miss > 0:
            logging.error(f'Primary key selected [{col} has missings values -> {df_col_miss}]')
        if df_col_dups > 0:
            logging.error(f'Primary key selected [{col} has duplicated values]')
            
    #3. Log datatypes
    dtypes = dataframe.dtypes.to_dict()
    for k, v in dtypes.items():
        logging.info(f'Column {col} has data type {v}')
        
    #4. Basic stats for numerical columns
    stats = dataframe.describe().to_dict()
    for k, v in stats.items():
        logging.info(f'Statistics for column {col}: {v}')

        