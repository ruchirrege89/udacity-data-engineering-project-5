from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator loads the fact table from staging redshift tables.
    
    Parameters:
    redshift_conn_id= redshift connection to access staging redshift tables
    table= Name of the fact table
    select_sql= query to populate fact table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('LoadFactOperator is implemented below')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data in fact table")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift.run(table_insert_sql)
