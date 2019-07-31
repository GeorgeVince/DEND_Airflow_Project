from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql = SqlQueries.insert_sql

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                table='',
                sql_select='',
                truncate = True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.sql_select = sql_select,
        self.truncate = self.truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncate enabled - truncating fact table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        #Append to the current dimension table
        self.log.info("Inserting into {}".format(self.table))
        
        sql = sql.format(
            self.table,
            self.sql_select
            )
        
        redshift.run(sql)
