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
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    song_df=spark.read.json(song_data)
    song_df.printSchema()
    
    # create temporary view to execute SQL queries
    song_df.createorReplaceTempview("songs_staging_table")

    # extract columns to create songs table
    songs_table = spark.sql(""" SELECT DISTINCT s.song_id,s.title,s.artist_id,s.year,s.duration
                                FROM songs_staging_table AS s
                                WHERE s.song_id is NOT NULL """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT s.artist_id,s.artist_name,s.artist_location,s.artist_latitude,s.artist_longitude
                                 FROM songs_staging_table AS s
                                 WHERE s.artist_id is NOT NULL""")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data +"log_data/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    log_df.printSchema()
    
    # filter by actions for song plays
    log_df = log_df.filterBy(log_df.page=="NextSong")

    # create temporary view to execute SQL queries
    log_df.createorReplaceTempview("log_staging_table")

    # extract columns for users table    
    users_table = spark.Sql("""SELECT DISTINCT l.userId,l.firstName,l.lastName,l.gender,l.level
                               FROM log_staging_table AS l
                               WHERE l.userId is NOT NULL""")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users_table/")

    
    # extract columns to create time table
    time_table = spark.sql(""" select subquery.starttime_sub as start_time, 
    hour(subquery.starttime_sub) as hour, 
    dayofmonth(subquery.starttime_sub) as day, 
    weekofyear(subquery.starttime_sub) as week, 
    month(subquery.starttime_sub) as month, 
    year(subquery.starttime_sub) as year, 
    dayofweek(subquery.starttime_sub) as weekday 
    from 
    (select totimestamp(l.ts/1000) 
    as starttime_sub from 
    log_staging_table l 
    where timestamp.ts IS NOT NULL ) subquery """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data +"time_table/")

    # read in song data to use for songplays table
    song_data = input_data+'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id AS songplay_id, 
                                    totimestamp(l.ts/1000) AS start_time, 
                                    l.userId AS user_id,
                                    l.level AS level,
                                    s.song_id AS song_id,
                                    s.artist_id AS artist_id,
                                    l.sessionId AS session_id,
                                    s.artist_location AS location,
                                    l.userAgent AS useragent,
                                    FROM log_staging_table AS l
                                    JOIN songs_staging_table AS s
                                    ON s.artist_name=l.artist
                                    AND s.title=l.song """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays_table/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
