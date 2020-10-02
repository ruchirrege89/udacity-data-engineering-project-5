from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator loads the dimension tables from staging redshift tables.
    
    Parameters:
    redshift_conn_id= redshift connection to access staging redshift tables
    table= Name of the dimension table
    select_sql= query to populate dimension table
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_insert = append_insert

    def execute(self, context):
        self.log.info('LoadDimensionOperator is implemented below')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_insert:
            self.log.info("Trucating data in dimension table")
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info("Loading data in dimension table")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift.run(table_insert_sql)
