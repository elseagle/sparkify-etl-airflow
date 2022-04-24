from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 fmt: [str],
                 query,
                 failure_value,
                 *args, **kwargs):
        """ Operator for checking the data quality. This currently runs the provided
        query for every argument in `fmt` and raises an exception if the `failure_value` 
        is returned. The SQL query is expected to return a single value.

        param conn_id: the Postgres Connection Id to use
        param fmt (list): A list of format args to fill the query with, for example, 
            several tables over which to run the same query.
        param query: The SQL query to run templated by the values in `fmt`
        param failure_value: throw exceptio if the failure value is hit 
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.fmt = fmt
        self.query = query
        self.failure_value = failure_value

    def execute(self, context):
        self.log.info('Running DataQualityOperator')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for f in self.fmt:
            query = self.query.format(f)
            res = redshift.get_first(query)[0]

            if res == self.failure_value:
                raise ValueError(f"failed query {query}, failure {self.failure_value}")
