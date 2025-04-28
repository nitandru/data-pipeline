from pipelines.data_validation_pipeline import ValidationPipeline

program = ValidationPipeline(
    df_path=r'C:\Users\andrei.nita\Desktop\CET\3_output\123\exports\filtered\inter_123_filt.csv',
    # db_table_name='cross_street',
    # model_class='CrossStreet',  # your SQLAlchemy model
    # existent_db_table=True,
    # schema=cross_street_schema,
    pk_columns=['CrossSt_ID', 'GRID_ID'],
    type_defaults={str: '', int: -9999, float: -9999.0}
)

program.run()