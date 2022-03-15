from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    
    
    """LoadFactOperator utilizes the SQL helper class to run data transformations. 
       Most of the logic is within the SQL transformations, 
       and the operator is expected to take as input a SQL statement, 
       and target database on which to run the query against.
    """

    ui_color = '#80BD9E'
    
    sql_insert="""INSERT INTO {}
                  {};
               """
    sql_truncate="""TRUNCATE TABLE {};
               """
    @apply_defaults
    def __init__(self,
                 # Define operator's params 
                 redshift_conn_id="",
                 dim_table="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('Executing LoadDimensionOperator')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        #Allow switch between append and insert-delete functionality
        
        if self.truncate_table :
            self.log.info(f'truncating table {self.dim_table}')
            redshift_hook.run(LoadFactOperator.sql_truncate.format(self.dim_table))
            self.log.info(f'loading data to the table {self.dim_table}')
            redshift_hook.run(LoadFactOperator.sql_insert.format(self.dim_table, load_sql_stmt))
        
        else:
            self.log.info(f'Appending the table {self.dim_table}')
            redshift_hook.run(LoadFactOperator.sql_insert.format(self.dim_table, load_sql_stmt))