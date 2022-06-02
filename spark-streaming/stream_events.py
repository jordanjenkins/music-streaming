# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_events.py

import os
from schema import schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, year, udf

# Kafka Topics

LISTEN_EVENTS_TOPIC = 'listen_events'
PAGE_VIEW_EVENTS_TOPIC = 'page_view_events'
AUTH_EVENTS_TOPIC = 'auth_events'

KAFKA_PORT = '9092'

KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS', 'localhost')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET', 'music-streams')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

def create_or_get_spark_session(app_name, master='yarn'):

    spark = (SparkSession
            .builder
            .appName(app_name)
            .master(master = master)
            .getOrCreate())

    return spark

spark = create_or_get_spark_session('Eventsim Stream')
spark.streams.resetTerminated()

def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset = 'earliest'):
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
    if s:
        return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
                .decode('unicode-escape') # Perform the actual octal-escaping decode
                .encode('latin1')         # 1:1 mapping back to bytes
                .decode(encoding)         # Decode original encoding
                .strip('\"'))

    else:
        return s

def process_stream(stream, stream_schema, topic):
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

# Create Streams for each event type
listen_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)

listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

page_view_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_ADDRESS, PAGE_VIEW_EVENTS_TOPIC)

page_view_events = process_stream(
    page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

auth_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)

auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)


def create_file_write_stream(stream, storage_path, checkpoint_path, trigger = '120 seconds', output_mode = 'append', file_format = 'parquet'):
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy('month', 'day', 'hour')
                    .option('path', storage_path)
                    .option('checkpointLocation', checkpoint_path)
                    .trigger(processingTime = trigger)
                    .outputMode(output_mode))
    
    return write_stream

write_listen_events = create_file_write_stream(listen_events,
                                                f'{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}',
                                                f'{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}'
                                                )

write_page_view_events = create_file_write_stream(page_view_events,
                                                f'{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}',
                                                f'{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}'
                                                )

write_auth_events = create_file_write_stream(auth_events,
                                                f'{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}',
                                                f'{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}'
                                                )

write_listen_events.start()
write_page_view_events.start()
write_auth_events.start()

spark.streams.awaitAnyTermination()