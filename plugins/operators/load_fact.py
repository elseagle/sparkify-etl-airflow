from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

 
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    upsert_sql = """
         START TRANSACTION;

        CREATE TABLE {stage} AS ({query});

        DELETE FROM {dest}
         USING {stage}
         WHERE {dest}.{primary_key} = {stage}.playid;

        INSERT INTO {dest}( playid, start_time, userid, "level",songid,artistid,sessionid,location,user_agent)
        SELECT * FROM {stage};

        DROP TABLE {stage};
        
        END TRANSACTION;
    """

    @apply_defaults
    def __init__(self,
                 conn_id,
                 dest_tbl,
                 primary_key,
                 query,
                 *args, **kwargs):
        """ Operator for Loading Fact Tables. This does an upsert
        making sure no duplicates on the primary_key are added.

        :param conn_id: the Postgres Connection Id to use
        :param dest_tbl: The name of the destination table 
        :param query: the SQL query that generates the dataset
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.query = query
        self.dest_tbl = dest_tbl
        self.primary_key = primary_key


    def execute(self, context):
        self.log.info('Running LoadFactOperator for {}'.format(self.dest_tbl))

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        # handle case where the schema is provided
        staging_table_name = "_staging_" + self.dest_tbl.replace('.', '_')

        redshift.run(LoadFactOperator.upsert_sql.format(stage=staging_table_name,
                                                        dest=self.dest_tbl, 
                                                        query=self.query,
                                                        primary_key=self.primary_key))
