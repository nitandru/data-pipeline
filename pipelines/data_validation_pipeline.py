import os
import logging
from typing import Any
import pandera as pa
from pandera import DataFrameSchema

from pipelines.utils import explore_data as helper_explore_data, read_data, remove_duplicates, drop_columns


logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)


class ValidationPipeline:
    def __init__(
        self,
        df_path: str,
        pk_columns: list[str] = None,
        columns_to_remove: list[str] = None,
        type_defaults: dict[type, Any] = None,

    ):
        self.df_path = df_path
        self.pk_columns = pk_columns
        self.columns_to_remove = columns_to_remove
        self.type_defaults = type_defaults or {str: '', int: -9999, float: -9999.0}
    def __repr__(self):
        return logging.info(f"Working on {self.df_path}")
    
    def read_data(self):
        self.df = read_data(self.df_path)
        return self.df
        
    def view_data(self):
        helper_explore_data(self.df, self.pk_columns, include_dtypes=False, include_summary=False)
        
    def remove_duplicates(self):
        for col in self.columns_to_remove:
            if col in self.pk_columns:
                raise TypeError(f'❌ Cannot delete {col}! This was assigned as primary key')
        self.df = remove_duplicates(self.df, self.pk_columns)
        return self.df
    
    def drop_columns(self):
        self.df = drop_columns(self.df, self.columns_to_remove)
    
    def run(self):
        try:
            self.read_data()
            self.view_data()
            self.remove_duplicates()
            self.drop_columns()
        except Exception as err:
            logging.error(f'Something went wrong: {err}')
        
    
    