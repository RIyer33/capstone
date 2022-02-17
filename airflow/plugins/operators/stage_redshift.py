from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 destination_table = "",
                 s3_bucket = "",
                 s3_key = "",
                 #role_arn = "",
                 aws_region = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.destination_table = destination_table
        self.s3_source_path = f"s3://{s3_bucket}/{s3_key}"
        #self.role_arn = role_arn
        self.aws_region = aws_region

    def build_cmd(self, credentials):
        copy_cmd = f"""
        COPY {self.destination_table}
        FROM '{self.s3_source_path}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        FORMAT AS PARQUET
        COMPUPDATE OFF;
        """
        self.log.info(f"Copy command: {copy_cmd}")
        return copy_cmd
    
    def execute(self, context):
        self.log.info('\n*** Cap - StageToRedshiftOperator: Copying data from S3 to Redshift ***')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            f"Copying data from {self.s3_source_path} to Redshift {self.destination_table} table"
        )
        redshift.run(self.build_cmd(credentials))
        self.log.info("*** Cap - StageToRedshiftOperator: Complete ***\n")