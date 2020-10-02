from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This operator checks if the tables are loaded with data.
    
    Parameters:
    redshift_conn_id= redshift connection to access staging redshift tables
    table[]= list of all tables
    select_sql= query to check count of rows in tables
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator is implemented below')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            table_quality_sql = f"""
            SELECT count(*) from {table}
            """
            records = redshift_hook.run(table_quality_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Error: Table {table} returns no results')
            if records[0][0] < 1:
                raise ValueError(f'Error: Table {table} contains 0 records')
            else:
                self.log.info("Data quality check passed")



        
