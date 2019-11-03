from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    create_user_sql =  """CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
)""" 
    
    insert_user_sql = """insert into user_table(
                            userid,
                            first_name,
                            last_name,
                            gender,
                            level)                          
        
        SELECT song_id, title, artist_id, year, duration FROM staging_songs"""
    
    create_artist_sql = """CREATE TABLE IF NOT EXIST artists_table (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
)""" 
    
    insert_artist_sql = ("""
        INSERT INTO artists (
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude    
        )
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)
    
    create_song_sql ="""CREATE TABLE IF NOT EXIST public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
)""" 
    
    insert_song_sql = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    create_time_sql = """CREATE TABLE IF NOT EXIST public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
) """ 
    
    insert_time_sql = ("""
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 region="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id,
        self.aws_credentials_id = aws_credentials_id,
        self.region = region,
        self.table = table,
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #user table 
        redshift.run(create_user_sql)
        
        redshift.run(insert_user_sql)
        
        #artist table 
        redshift.run(create_artist_sql)
        
        redshift.run(insert_artist_sql)
        
        #song table 
        
        redshift.run(create_song_sql)
        
        redshift.run(insert_song_sql)
        
        #time table 
        
        redshift.run(create_time_sql)
        
        redshift.run(insert_time_sql)
        
        
