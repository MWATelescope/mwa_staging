"""
Daemon to run continuously, receiving Kafka messages about files that have been staged.

Updates the 'staging_jobs' and 'files' tables in the database, and pushes a notification to ASVO
when all files in a job have been staged.
"""

from configparser import ConfigParser as conparser
import json
import traceback

from kafka import KafkaConsumer

import psycopg2
from psycopg2 import extras

import requests

MWA_TOPIC = "mwa"    # Kafka topic to listen on
KAFKA_SERVERS = ['localhost:9092']

CPPATH = ['/usr/local/etc/staging.conf', '/usr/local/etc/staging-local.conf',
          './staging.conf', './staging-local.conf']
CP = conparser(defaults={})
CPfile = CP.read(CPPATH)
if not CPfile:
    print("None of the specified configuration files found by mwaconfig.py: %s" % (CPPATH,))

DB = psycopg2.connect(user=CP['default']['dbuser'],
                      password=CP['default']['dbpass'],
                      host=CP['default']['dbhost'],
                      database=CP['default']['dbname'])


def HandleMessages(consumer):
    """
    Runs forever, processing Kafka messages one by one. Exits if there's an exception
    in the message handling code.

    :return:
    """
    for msg in consumer:
        try:
            # do stuff
        except:
            print(traceback.format_exc())
            return
        finally:
            consumer.commit()


if __name__ == '__main__':
    consumer = KafkaConsumer(MWA_TOPIC,
                             bootstrap_servers=KAFKA_SERVERS,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             group_id='mwa_staging',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    HandleMessages(consumer)