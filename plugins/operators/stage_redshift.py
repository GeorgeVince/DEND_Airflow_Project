from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket = '',
                 s3_key = '',
                 aws_conn_id = '',
                 redshift_conn_id ='',
                 table='',
                 copy_query='',
                 json_params='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.copy_query = copy_query
        self.json_params = json_params


    def execute(self, context):
        """Drop existing Redshift table, Create and copy into new Redshift table"""
        
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Populating {} in redshift".format(self.table))
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        copy_sql = self.copy_query.format(self.table,
                                          s3_path,
                                          credentials.access_key,
                                          credentials.secret_key,
                                          self.json_params)

        self.log.info("Executing {}".format(copy_sql))
        redshift.run(copy_sql)






