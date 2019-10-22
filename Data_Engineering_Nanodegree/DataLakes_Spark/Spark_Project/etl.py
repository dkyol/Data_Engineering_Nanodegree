import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_view")

    # extract columns to create songs table
    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM song_view")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+'outputlocationsong/')

    # extract columns to create artists table
    artists_table = spark.sql("SELECT artist_id, artist_location, artist_latitude, artist_longitude FROM song_view") 
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output__data+'outputLocationArtist')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView("log_table")

    # extract columns for users table    
    user_table = spark.sql('SELECT userId, firstName, lastName, gender, level FROM log_view')
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(output_data +'userTableOutput/')

    # create time table 
    
    dftime = spark.sql("SELECT ts FROM log_view")
    
    # create udf to convert timestamp 
    
    @udf
    def get_timestamp(ms):

        import datetime

        sec = int(ms)/1000

        result = datetime.datetime.fromtimestamp(sec).strftime('%Y-%m-%d %H:%M:%S.%f')

        return result
    
    # add new formatted time stamp
    
    dftime.withColumn("datetime", get_timestamp("ts"))

    # create time view 
    
    dftime.createOrReplaceTempView("time")
    
    # extract columns to create time table
    time_table = spark.sql("SELECT distinct(ts), datetime FROM time")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data+'time_table/')

    # read in song data for songplay table
    song_df = spark.sql("SELECT datatime FROM  ")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
                    """SELECT datetime, userId, level, song_id, artist_id, sessionId, location, userAgent
                          FROM time
                      JOIN log_view
                          ON time.ts == log_view.ts
                      JOIN song_view
                          ON song_view.artist_name == log_view.artist
                      """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output_data+'songplay_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
