from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Loading fact table")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.query
        )
        redshift.run(formatted_sql)

