from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#89DA59'
    
    

    upsert_sql = """
        START TRANSACTION;

        CREATE TABLE {stage} AS ({query});

        DELETE FROM {dest}
         USING {stage}
         WHERE {dest}.{primary_key} = {stage}.{primary_key};

        INSERT INTO {dest}
        SELECT * FROM {stage};

        DROP TABLE {stage};
        
        END TRANSACTION;
    """

    insert_sql = """
        INSERT INTO {}
        {}
    """
    @apply_defaults
    def __init__(self,
                 conn_id,
                 dest_tbl,
                 query,
                 primary_key=None,
                 mode="APPEND",
                 *args, **kwargs):
        """ Operator for Loading Dimension Tables. If the mode is APPEND,
        this will perform an upsert (update insert), first deleting any
        matching rows between the query output and the destination table 
        (joined on `primary_key`) and then inserting all rows from the
        query.

        param conn_id: the Postgres Connection Id to use
        param dest_tbl: The name of the destination table
        param query: the SQL query that generates the dataset
        param primary_key: The primary key to use for the upsert (Append mode).
        param mode: the mode, either APPEND or TRUNCATE, the later
            deleting the content of the table before insertion.
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.query = query
        self.mode = mode
        self.dest_tbl = dest_tbl
        self.primary_key = primary_key

        if mode not in ["APPEND", "TRUNCATE"]:
            raise "ValueError"

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator for {} - {}'.format(self.dest_tbl, self.mode))

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.mode == "TRUNCATE":
            redshift.run(f"TRUNCATE {self.dest_tbl}")

            redshift.run(LoadDimensionOperator.insert_sql.format(self.dest_tbl, self.query))
        else:

            # handle case where the schema is provided
            staging_table_name = "_staging_" + self.dest_tbl.replace('.', '_')

            redshift.run(LoadDimensionOperator.upsert_sql.format(stage=staging_table_name,
                                                                 dest=self.dest_tbl, 
                                                                 query=self.query,
                                                                 primary_key=self.primary_key))

