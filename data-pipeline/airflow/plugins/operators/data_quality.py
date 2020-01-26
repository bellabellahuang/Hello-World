from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_sql="",
                 expected_result=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_sql = data_quality_sql
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(self.data_quality_sql)
        if len(records) < 1 or len(records[0]) < 1:
            result = 0
        else:
            result = records[0][0]
        if result != self.expected_result:
            raise ValueError(f"Data quality check failed. {self.data_quality_sql} returned {result}")
        logging.info(f"Data quality check passed for {self.data_quality_sql} with result {result}")