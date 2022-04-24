from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ['s3_key']

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 jsonpath='auto',
                 *args, 
                 **kwargs):
        """ Operator that loads data from S3 to Redshift

        :param redshift_conn_id: The connection Id for the Postgres Connection
        :param table: Name of the destination table
        :param s3_bucket: name of the Bucket
        :param s3_key: Name of the key within the bucket
        :param jsonpath: S3 link to a jsonpath if available, else auto
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.jsonpath = jsonpath


    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear the table first
        redshift.run("DELETE FROM {}".format(self.table))

        # Load the data from the rendered s3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.log.info('Loading data from {} to Redshift table {}'.format(s3_path, self.table))

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.jsonpath
        )
        redshift.run(formatted_sql)


