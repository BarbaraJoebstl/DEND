from configparser import ConfigParser
from os import path
from configparser import NoSectionError, NoOptionError, ParsingError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_profile_credentials(profile_name):
    config = ConfigParser()
    config.read([path.join(path.expanduser("~"), '.aws/credentials')])
    try:
        aws_access_key_id = config.get(profile_name, 'aws_access_key_id')
        aws_secret_access_key = config.get(
            profile_name, 'aws_secret_access_key')
    except ParsingError:
        print('Error parsing config file')
        raise
    except (NoSectionError, NoOptionError):
        try:
            aws_access_key_id = config.get('default', 'aws_access_key_id')
            aws_secret_access_key = config.get(
                'default', 'aws_secret_access_key')
        except (NoSectionError, NoOptionError):
            print('Unable to find valid AWS credentials')
            raise
    return aws_access_key_id, aws_secret_access_key


aws_access_key_id, aws_secret_access_key = get_profile_credentials('joebsbar')


def create_spark_session():
    """
    Creates a new spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Takes in song data from a given destination, extracts parquet files and outputs it to a given destination.

    Params:
        spark: the cursor object
        input_data: path to data to process (in our case song data)
        output_data: path where parquet files will be stored
    """

    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print('---reading song data from s3')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        f'{output_data}/songs_table', mode='overwrite', partitionBy=['year', 'artist_id'])
    print('---songs table to parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'title', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(
        f'{output_data}/artists_table', mode='overwrite')
    print('---artists table to parquet')


def process_log_data(spark, input_data, output_data):
    """
    Takes in song data from a given destination, extracts parquet files and outputs it to a given destination.

    Params:
        spark: the cursor object
        input_data: path to data to process (in our case log data)
        output_data: path where parquet files will be stored
    """

    # get filepath to log data file
    log_data = f'{input_data}/log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    print('---reading log data from s3')

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName',
                            'lastName', 'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(f'{output_data}/users_table', mode='overwrite')
    print('---users table to parquet')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    time_data = df.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))

    # extract columns to create time table
    time_data = time_data.select('ts', 'start_time') \
        .withColumn('year', F.year('start_time')) \
        .withColumn('month', F.month('start_time')) \
        .withColumn('week', F.weekofyear('start_time')) \
        .withColumn('weekday', F.dayofweek('start_time')) \
        .withColumn('day', F.dayofyear('start_time')) \
        .withColumn('hour', F.hour('start_time')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_data.write.parquet(
        'f{output_data}/time_table', mode='overwrite', partitionBy=['year', 'month'])
    print('---time table to parquet')

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    song_data = spark.read.json(song_data)
    print('---read song data')

    song_data.createOrReplaceTempView('song_data')
    log_data.createOrReplaceTempView('log_data')
    time_data.createOrReplaceTempView('time_data')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(""" SELECT DISTINCT
                                    l.ts as songplay_id,
                                    l.ts as start_time,
                                    l.userId as user_id,
                                    l.level as level,
                                    s.song_id as song_id,
                                    s.artist_id as artist_id,
                                    l.sessionId as session_id,
                                    l.location as location,
                                    l.userAgent as user_agent
                                FROM song_data s
                                JOIN log_data l
                                    ON s.artist_name = l.artist
                                    AND s.title = l.song
                                    ANd s.duration = l.length
                                JOIN time_data t
                                    ON t.ts = l.ts
                                """).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        f'{output_data}/songplays_table', mode='overwrite', partition=['year', 'month'])
    print('--- songplays to parquet files')


def main():
    """
    create a spark session,
    loads data from the give s3 path
    process song and log data and creates new tables
    uploads the newly created tables to a given s3 path
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-a4-barbara/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
