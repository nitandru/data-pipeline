import os
import logging
from typing import Any
import pandera as pa
from pandera import DataFrameSchema
from pipelines.helper_functions import read_data, explore_data as helper_explore_data

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)


class ValidationPipeline:
    def __init__(
        self,
        df_path: str,
        # db_table_name: str,
        # model_class: Any = None,
        # existent_db_table: bool = False,
        # schema: DataFrameSchema = None,
        pk_columns: list[str] = None,
        type_defaults: dict[type, Any] = None,
        # session: Session = None,
        # batch_size: int = 1000,
        # failed_batches_dir: str = "failed_batches"
    ):
        self.df_path = df_path
        # self.db_table_name = db_table_name
        # self.model_class = model_class
        # self.existent_db_table = existent_db_table
        # self.schema = schema
        self.pk_columns = pk_columns or []
        self.type_defaults = type_defaults or {str: '', int: -9999, float: -9999.0}
        # # self.session = session
        # self.batch_size = batch_size
        # self.failed_batches_dir = failed_batches_dir
        # self.df = None

    def __repr__(self):
        return logging.info(f"Working on {self.df_path}")
    
    def read_data(self):
        self.df = read_data(self.df_path)
        return self.df
        
    def explore_data(self):
        helper_explore_data(self.df, self.pk_columns)
    
    def run(self):
        try:
            self.read_data()
            self.explore_data()
        except Exception as err:
            logging.error(f'Something went wrong: {err}')
        
    
    