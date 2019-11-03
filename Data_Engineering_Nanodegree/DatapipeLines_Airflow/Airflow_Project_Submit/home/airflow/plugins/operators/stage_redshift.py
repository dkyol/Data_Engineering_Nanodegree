from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    
    
    ui_color = '#358140'
    # from is the S3 bucket path 
    
    #https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
           
                 redshift_conn_id="",
                 aws_credentials_id="",
                 region="",
                 table="",
                 s3_bucket="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table,
        self.redshift_conn_id = redshift_conn_id,
        self.s3_bucket = s3_bucket,
        self.aws_credentials_id = aws_credentials_id,
        self.region=region,
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
       
        aws_hook = AwsHook(self.aws_credentials_id)
               
        
        #credentials = session.get_credentials()
        #credentials = credentials.get_frozen_credentials()
        credentials = aws_hook.get_credentials()      
        
        self.log.info("Log this variable ...{}".format(credentials))
        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Create tables staging tables...")
        
        redshift.run("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist varchar(256),
                                    auth varchar(256),
                                    firstname varchar(256),
                                    gender varchar(256),
                                    iteminsession int4,
                                    lastname varchar(256),
                                    length numeric(18,0),
                                    "level" varchar(256),
                                    location varchar(256),
                                    "method" varchar(256),
                                    page varchar(256),
                                    registration numeric(18,0),
                                    sessionid int4,
                                    song varchar(256),
                                    status int4,
                                    ts int8,
                                    useragent varchar(256),
                                    userid int4)
                          """)
        
        redshift.run("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs int4,
                                    artist_id varchar(256),
                                    artist_name varchar(256),
                                    artist_latitude numeric(18,0),
                                    artist_longitude numeric(18,0),
                                    artist_location varchar(256),
                                    song_id varchar(256),
                                    title varchar(256),
                                    duration numeric(18,0),
                                    "year" int4
)""")

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        #rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}".format(self.s3_bucket) #, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            'AKIAJ5QF3MAXCO53LIXQ',
            'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            
        ) # copy_eventssql_json.format
        redshift.run(formatted_sql)
        
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")






