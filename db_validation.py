import os
import logging
from typing import Any
import pandera as pa
from pandera import DataFrameSchema

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

from pipeline.helper_functions import read_data, explore_data

class ValidationPipeline:
    def __init__(
        self,
        df_path: str,
        db_table_name: str,
        model_class: Any,
        existent_db_table: bool,
        schema: DataFrameSchema,
        pk_columns: list[str],
        type_defaults: dict[type, Any],
        session: Session,
        batch_size: int = 10000,
        failed_batches_dir: str = "failed_batches"
    ):
        self.df_path = df_path
        self.db_table_name = db_table_name
        self.model_class = model_class
        self.existent_db_table = existent_db_table
        self.schema = schema
        self.pk_columns = pk_columns
        self.type_defaults = type_defaults
        self.session = session
        self.batch_size = batch_size
        self.failed_batches_dir = failed_batches_dir
        self.df = None

    def __repr__(self):
        return logging.info(f"Working on {self.df_path}")
    
    def read_data(self):
        self.df = read_data(self.df_path)
        return self.df
        
    def explore_data(self):
        self.explore_data()
    
    def run(self):
        self.read_data()
        self.explore_data()
        
    
    