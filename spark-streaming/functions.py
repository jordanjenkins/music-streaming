from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, year, udf

def create_or_get_spark_session(app_name, master = 'yarn'):
    """
    Creates or gets a Spark session

    Parameters:
        app_name: str
            The name of the app
        master: str
            Specify the Spark master, default is YARN
    Returns:
        spark: SparkSession

    """
    spark = (SparkSession
            .builder
            .appName(app_name)
            .master(master = master)
            .getOrCreate())

    return spark

def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset = 'earliest'):
    """
    Create a Kafka read stream

    Parameters:
        spark: SparkSession
            A SparkSession object
        kafka_address: str
            Host address of the kafka bootstrap server
        kafka_port: str
            The kafka port
        topic: str
            Name of kafka topic
        starting_offset: str
            Starting offset configuration, default is earliest
    Returns:
        read_stream: DataStreamReader
    """

    read_stream = (spark
                    .readStream
                    .format('kafka')
                    .option('kafka.bootstrap,servers', f'{kafka_address}:{kafka_port}')
                    .option('failOnDataLoss', False)
                    .option('startingOffsets', starting_offset)
                    .option('subscribe', topic)
                    .load())
    return read_stream

@udf
def string_decode(s, encoding='utf-8'):
    """
    Correct stream encoding

    Parameter:
        s: str
            The string to be decoded
    Returns:
        s: str
    """

    if s:
        return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
                .decode('unicode-escape') # Perform the actual octal-escaping decode
                .encode('latin1')         # 1:1 mapping back to bytes
                .decode(encoding)         # Decode original encoding
                .strip('\"'))

    else:
        return s

def process_stream(stream, stream_schema, topic):
    """
    Process kafka stream by value. Convert `ts` to timestamp and generate year, month, day, hour
    Decode song and artist from listen_events and page_view_events.

    Parameters:
        stream: DataStreamReader
            The data stream reader to be processed
        stream_schema: dict
            Value from dict defining the data schema
        topic: str
            The kafka topic to be processed
    Returns:
        stream: DataStreamReader
    """
    stream = (stream
              .selectExpr('CAST(value as STRING')
              .select(
                  from_json(col('value'), stream_schema).alias(
                      'data'
                  )
              )
              .select('data.')
              )
    
    stream = (stream
             .withColumn('ts', (col('ts')/1000).cast('timestamp'))
             .withColumn('year', year(col('ts')))
             .withColumn('month', month(col('ts')))
             .withColumn('day', dayofmonth(col('ts')))
             .withColumn('hour', hour(col('ts')))
             )
    
    if topic in ['listen_events', 'page_view_events']:
        stream = (stream
                 .withColumn('song', string_decode('song'))
                 .withColumn('artist', string_decode('artist'))
                 )
    
    return stream

def create_file_write_stream(stream, storage_path, checkpoint_path, trigger = '120 seconds', output_mode = 'append', file_format = 'parquet'):
    """
    Write the stream to data lake.

    Parameters:
        stream: DataStreamReader
            The data stream reader to be written
        storage_path: str
            File output path
        checkpoint_path: str
            checkpoint location for spark
        trigger: str
            Trigger interval, how often the stream will be written to storage
        output_mode: str
            append, complete, update. Default is append
        file_format: str
            The file format to be written to the data lake, default is parquet
    """
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy('month', 'day', 'hour')
                    .option('path', storage_path)
                    .option('checkpointLocation', checkpoint_path)
                    .trigger(processingTime = trigger)
                    .outputMode(output_mode))
    
    return write_stream