from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """DataQualityOperator run  checks on the data itself. 
       The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. 
       For each the test, the test result and expected result needs to be checked and if there is no match, the operator raises an exception and
       the task should retry and fail eventually.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operator's params 
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id =  redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Check the result in the destination table and the number of rows in the destination table and raise message
        for table in self.tables:
            records=redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            if records is None or len(records[0])<1:
                self.log.error(f"Data quality check failed. Table {table} returned no result")
                raise ValueError (f"Data quality check failed. Table {table} returned no result")
                
            num_records = records[0][0]  
            if num_records <1 :
                self.log.error(f"Data quality check failed. Destination table {table} contained 0 rows")
                raise ValueError(f"Data quality check failed. Destination table {table} contained 0 rows")
            
            self.log.error(f"Data quality check passed. Destination table {table} contained {num_records} rows")
            raise ValueError(f"Data quality check passed. Destination table {table} contained {num_records} rows")
            
                
       