from pipelines.data_validation_pipeline import ValidationPipeline

program = ValidationPipeline(
    df_path=r'C:\Users\andrei.nita\Desktop\CET\3_output\123\exports\123_speed_allveh.csv',
    pk_columns=['tmc_code', 'Observation_Count'],
    columns_to_remove = ['Direction'],
    # db_table_name='cross_street',
    # model_class='CrossStreet',  # your SQLAlchemy model
    # existent_db_table=True,
    # schema=cross_street_schema,
    type_defaults={str: '', int: -9999, float: -9999.0}
)

program.run()