from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'  

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                dst_table='',
                sql_insert='',
                truncate=False,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dst_table = dst_table
        self.sql_insert = sql_insert
        self.truncate = truncate

    def execute(self, context):
        sql = SqlQueries.insert_sql

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncate enabled - truncating fact table")
            redshift.run("TRUNCATE TABLE {}".format(self.dst_table))
       
        #Append to the 
        self.log.info("Inserting into {}".format(self.dst_table))
        
        sql = sql.format(
            self.dst_table,
            self.sql_insert
            )
        
        self.log.info(sql)
        
        redshift.run(sql)
