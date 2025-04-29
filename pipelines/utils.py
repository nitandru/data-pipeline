import pandas as pd
import logging
import os
from typing import Any
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def validate_columns(dataframe: pd.DataFrame, columns: list[str]) -> None:
    """Helper function that checks if columns argument is a list and all cols exists

    Args:
        dataframe (pd.DataFrame): The input DataFrame to check columns agains
        columns (list[str]): A list of columns
    Raises:
        TypeError: If columns is not a list or if any element is not a string.
        ValueError: If columns is empty or if any column is not in the DataFrame.
    """
    if not isinstance (columns, list):
        raise TypeError(f"Columns must be a list; got {type(columns).__name__}")
    if not columns:
        raise ValueError(f"Columns must not be an empty list")
    if not all(isinstance(col, str) for col in columns):
        raise TypeError("All column names must be strings")
    missing_cols = [col for col in columns if col not in dataframe.columns]
    if missing_cols:
        raise ValueError(f"Columns {missing_cols} not found in DataFrame. Available columns: {list(dataframe.columns)}")
    

def read_data(file_path: str) -> pd.DataFrame:
    """Read a file into a Pandas dataframe based on its extension type
    Credits to `https://gist.github.com/rfeers`
    
    Args:
        file_path (str): Path file to data

    Returns:
        _type_: pd.DataFrame
    """
    _, file_ext = os.path.splitext(file_path)
    if file_ext == '.csv':
        return pd.read_csv(file_path)
    elif file_ext == '.json':
        return pd.read_json(file_path)
    elif file_ext in ['.xls', '.xlsx']:
        return pd.read_excel(file_path)
    else:
        return logging.error(f'Format {file_ext} not recognized')
    

def explore_data(dataframe: pd.DataFrame, pk_columns: list[str], include_dtypes: bool = False, include_summary: bool = False) -> None:
    """Shows general information about a dataframe

    Args:
        dataframe (pd.DataFrame): The dataframe read
        pk_columns (list): Columns used to uniquely identify rows in the DataFrame.
    """
    validate_columns(dataframe, pk_columns)
    print(10 * "==", "Data exploration", 10 * "==")
    #Show shape
    shape = dataframe.shape
    logging.info(f'Dataframe has {shape[0]} rows and {shape[1]} columns')
    
    #1. Check missings
    nan_count = dataframe.isna().sum().to_dict()
    for k, v in nan_count.items():
        if v > 0:
            logging.warning(f'Column [{k}] has {v} ({v/shape[0]*100:.2f}%) missing observations')
    
    #2. Check missing and duplicates in pk_columns
    for col in pk_columns:
        df_col_miss = dataframe[col].isna().sum()
        df_col_dups = dataframe[col].duplicated().sum()
        logging.debug(df_col_dups)
        if df_col_miss > 0:
            logging.error(f'❕Primary key selected [{col}] has missings values -> {df_col_miss}')
            logging.info(10 * "===")
        if df_col_dups > 0:
            logging.error(f'❌ Primary key selected [{col}] has duplicated values')
                        
    #3. Log datatypes
    if include_dtypes:
        dtypes = dataframe.dtypes.to_dict()
        for k, v in dtypes.items():
            logging.info(f'Column {k} has data type {v}')
        
    #4. Basic stats for numerical columns
    if include_summary:
        stats = dataframe.describe().to_dict()
        for k, v in stats.items():
            logging.info(f'Statistics for column {k}: count: {v["count"]}; minimum:{v["min"]}; maximum: {v["max"]}')

    
def remove_duplicates(dataframe: pd.DataFrame, columns: list[str], verbose: bool = True) -> pd.DataFrame:
    """Removes duplicate rows from a DataFrame based on specified columns.
    Args:
        dataframe (pd.DataFrame): The input DataFrame from which duplicates will be removed.
        columns (list[str]): A list of column names to check for duplicates.
        verbose (bool, optional): _description_. Defaults to True.
        verbose (bool, optional): If True, logs the number of duplicates removed for each column. Defaults to True.
        pd.DataFrame: A DataFrame with duplicates removed.

    Returns:
        pd.DataFrame: _description_
    """
    validate_columns(dataframe, columns)
        
    print(10 * "==", "Duplicate removal", 10 * "==")
    if verbose:
        for col in columns:
            no_dups = dataframe.duplicated(subset=[col]).sum()
            logging.info(f'📌 Removed {no_dups} duplicates from columns [{col}]')
    # Remove duplicates
    dups = dataframe.duplicated(subset=columns)
    return dataframe[~dups]


def drop_columns(dataframe: pd.DataFrame, columns:list[str], inplace=False, verbose=True):
    """Drop a list of columns from a DataFrame

    Args:
        datarame (pd.DataFrame): The input DataFrame.
        columns (list[str]): List of columns to be dropped
        inplace (bool, optional): Defaults to False.
        verbose (bool, optional): Defaults to True.
    """
    validate_columns(dataframe,  columns)
    if verbose:
        logging.info(f"Removing {columns}...")
    return dataframe.drop(columns = columns, inplace=inplace)
        
    