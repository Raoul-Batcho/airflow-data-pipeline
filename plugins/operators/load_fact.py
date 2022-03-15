from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    
    """LoadFactOperator utilizes the SQL helper class to run data transformations. 
       Most of the logic is within the SQL transformations, 
       and the operator is expected to take as input a SQL statement, 
       and target database on which to run the query against.
    """

    ui_color = '#F98866'
    
    sql_insert="""INSERT INTO {}
                  {};
               """
    sql_truncate="""TRUNCATE TABLE {};
               """
    
    @apply_defaults
    def __init__(self,
                 # Define operator's params 
                 redshift_conn_id="",
                 fact_table="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table
        
    def execute(self, context):
        self.log.info('Executing LoadFactOperator')
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        #Allow switch between append and insert-delete functionality
        
        if self.truncate_table :
            redshift_hook.run(LoadFactOperator.sql_truncate.format(self.fact_table))
            redshift_hook.run(LoadFactOperator.sql_insert.format(self.fact_table, load_sql_stmt))
        
        else:
            redshift_hook.run(LoadFactOperator.sql_insert.format(self.dim_table, load_sql_stmt))
