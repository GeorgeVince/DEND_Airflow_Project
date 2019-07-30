from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                table='',
                create_query='',
                sql_insert='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.sql_insert = sql_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping {} from redshift (if exists)".format(self.table))
        redshift.run("drop table if exists {}".format(self.table))

        self.log.info("Creating {} in redshift".format(self.table))
        redshift.run(self.create_query)

        self.log.info("Inserting into {}".format(self.table))
        redshift.run(self.sql_insert)
