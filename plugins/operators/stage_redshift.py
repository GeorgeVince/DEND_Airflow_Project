from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket = '',
                 s3_key = '',
                 aws_conn_id = '',
                 redshift_conn_id ='',
                 table='',
                 create_query='',
                 copy_query='',
                 *args, **kwargs)

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3.s3_key
        self.aws_conn_id = aws.aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_query = create_query
        self.copy_query = copy_query


    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping {} from redshift".format(self.table))
        redshift.run("drop table if exists {}".format(self.table))




