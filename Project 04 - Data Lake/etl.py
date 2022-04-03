import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Initiate an Apache Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data files from an S3 bucket. Mapped appropriate columns
    in the song data files to songs and artist table.
    
    Params
    spark: Apache Spark session initiated previously.
    input_data: path to song data file
    output_data: S3 bucket path where the processed data will be stored."""
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id', 'year', 'duration') \
                    .dropDuplicates() 
                
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', \
                              'artist_latitude', 'artist_longitude') \
                      .dropDuplicates() \
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    """Read log data files from an S3 bucket. Mapped appropriate columns
    in the log data files to users, time, and songplays table. 
    
    Params
    spark: Apache Spark session initiated previously.
    input_data: path to song data file
    output_data: S3 bucket path where the processed data will be stored."""
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName', 'gender', 'level') \
                    .dropDuplicates() 
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df2.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        dayofweek('datetime').alias('weekday')) \
        .dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data,'time'), partitionBy = ['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*', '*.json'))

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, song_df.title == df.song)
    
    songplays_table = new_df.select(
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent')) \
        .withColumn('songplay_id', F.monotonically_increasing_id())

    # Reordering Columns
    songplays_table = songplays_table.select('songplay_id' ,'start_time', 'user_id', 'level', 'song_id', \
                                             'artist_id', 'session_id', 'location', 'user_agent', \
                                             'year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy(['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://udacity-dend/loaded_data'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
