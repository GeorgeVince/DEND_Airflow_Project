from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                tables_to_check = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_to_check = tables_to_check


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql = "SELECT COUNT(*) from {}"

        for table in self.tables_to_check:
            self.log.info("Checking {} has data...".format(table))
            formatted_sql = sql.format(table)
            records = redshift.get_records(formatted_sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise AssertionError("DATA QUALITY CHECK FAILED: {} - has no results".format(table))

            amount_of_records = records[0][0]

            if amount_of_records < 0:
                raise AssertionError("DATA QUALITY CHECK FAILED: {} - has 0 rows".format(table))

            self.log.info("Data quality checked passed on table {} - has {} records".format(table, amount_of_records))

        self.log.info("All data quality checks passed!")





            
