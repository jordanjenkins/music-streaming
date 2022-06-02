# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_events.py

import os
from schema import schema
from functions import *

# Kafka Topics

LISTEN_EVENTS_TOPIC = 'listen_events'
PAGE_VIEW_EVENTS_TOPIC = 'page_view_events'
AUTH_EVENTS_TOPIC = 'auth_events'

KAFKA_PORT = '9092'

KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS', 'localhost')
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET', 'music-streams')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

# Create Spark session
spark = create_or_get_spark_session('Eventsim Stream')
spark.streams.resetTerminated()

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

# Write streams to GCS
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