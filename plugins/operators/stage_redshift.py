from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This operator moves the data from s3 to stage redshift tables.
    
    Parameters:
    table= Table in redshift where data needs to be moved.
    redshift_conn_id= redshit connection
    s3_bucket= Name of the bucket where data is stored.
    s3_key= Exact path for the files.
    aws_credentials_id = aws credentials to login in.
    
    """
    ui_color = '#358140'


    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 json_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is implemented below')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete data from redshift staging table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copy data from S3 to Redshift")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format
        )
        redshift.run(formatted_sql)
