from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """StageToRedshiftOperator loads any JSON formatted files from S3 to Amazon Redshift. 
       The operator creates and runs a SQL COPY statement based on the parameters provided. 
       The operator's parameters specify where in S3 the file is loaded and what is the target table.
       The parameters distinguish between JSON file.
       The stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
    """
    ui_color = '#358140'
    
    sql_copy="""copy {}
                from '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}';
                {} 'auto'
             """
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 table="",
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 data_format="",
                 partitioning=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.data_format=data_format
        self.execution_date=kwargs.get('execution_date')
        self.partitioning=partitioning

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook=AwsHook(self.aws_credentials)
        credentials=aws_hook.get_credentials()     
        
        bucket=self.s3_bucket
        key=self.s3_key
        rendered_key=self.s3_key.format(**context)
        year=self.execution_date.strftime("%Y")
        month=self.execution_date.strftime("%m")
        
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        # If we are using partitioning, setup S3 path to use year and month of execution_date
        
        s3_path= f"s3://{bucket}/{key}/{year}/{month}" if self.partitioning else f"s3://{bucket}/{rendered_key}"
        
        redshift_hook.run(StageToRedshiftOperator.sql_copy.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.data_format)) 
                          
                          