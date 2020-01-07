import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}song_data/*/*/*/*.json'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df \
        .select("song_id", "title", "artist_id", "year", "duration") \
        .groupBy("song_id")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table \
        .write \
        .partitionBy("year", "artist_id") \
        .parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df \
        .selectExpr( \
            "artist_id", "artist_name as name", \
            "artist_location as location", \
            "artist_latitude as latitude", \
            "artist_longitude as longitude") \
        .groupBy("artist_id")
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{}log_data/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page")=="NextSong")

    # extract columns for users table    
    users_table = df \
        .selectExpr( \
            "userId as user_id", "firstName as first_name", \
            "lastName as last_name", "gender", "level") \
        .groupBy("user_id")
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf( \
        lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime columns from original timestamp column
    df = df \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("week", weekofyear("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("year", year("timestamp")) \
        .withColumn("weekday", date_format("timestamp", 'EEEE'))
    
    # extract columns to create time table
    time_table = df \
        .selectExpr( \
            "ts as start_time", "hour", "day", \
            "week", "month", "year", "weekday") \
        .groupBy("start_time")
    
    # write time table to parquet files partitioned by year and month
    time_table \
        .write \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark \
        .read \
        .parquet('{}songs/*/*/*.parquet'.format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
        .join(song_df, df.song == song_df.title) \
        .selectExpr("df.ts AS start_time", "df.user_id", "df.level", \
            "song_df.song_id", "song_df.artist_id", "df.session_id", \
            "df.location", "df.user_agent", "df.month", "df.year")

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    # input data path for local test
    # input_data = "data/"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-sparkify-datalake/"
    
    process_song_data(spark, input_data, output_data)   
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
