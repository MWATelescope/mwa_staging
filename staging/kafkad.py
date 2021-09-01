"""
Daemon to run continuously, receiving Kafka messages about files that have been staged.

Updates the 'staging_jobs' and 'files' tables in the database, and pushes a notification to ASVO
when all files in a job have been staged.
"""

from configparser import ConfigParser as conparser
import datetime
from datetime import timezone
import json
import threading
import time
import traceback

from kafka import KafkaConsumer

import psycopg2

import requests

MWA_TOPIC = "mwa"    # Kafka topic to listen on
KAFKA_SERVERS = ['localhost:9092']
ASVO_URL = 'http://localhost:8000/jobresult'

CHECK_INTERVAL = 60    # Check all job status details once every minute.
RETRY_INTERVAL = 600   # Re-try notifying ASVO about completed jobs every 10 minutes until we succeed
EXPIRY_TIME = 86400    # Return an error if it's been more than a day since a job was staged, and it's still not finished.

COMPLETION_QUERY = """
SELECT files.job_id, count(*), staging_jobs.total_files, staging_jobs.completed
FROM files JOIN staging_jobs USING(job_id)
WHERE ready AND (not staging_jobs.notified) 
GROUP BY (files.job_id, staging_jobs.total_files, staging_jobs.completed)
"""

CPPATH = ['/usr/local/etc/staging.conf', '/usr/local/etc/staging-local.conf',
          './staging.conf', './staging-local.conf']
CP = conparser(defaults={})
CPfile = CP.read(CPPATH)
if not CPfile:
    print("None of the specified configuration files found by mwaconfig.py: %s" % (CPPATH,))


def process_message(msg, db):
    """
    Handle a single message from Kafka, when a new file has been staged. Here msg.value has already
    been de-serialised, so it's a Python dictionary, with contents TBD.

    :param msg: Named Tuple from Kafka, with attributes 'topic', 'partition', 'offset', 'key' and 'value'
    :param db: Database connection object
    :return: Number of rows updated in the files table
    """
    filename = msg.value.get('filename', None)
    if filename is None:
        print("Invalid message, no 'filename' key: %s" % msg.value)
        return '', 0
    query = "UPDATE files SET ready=true, readytime=now() WHERE filename=%s"
    with db:
        with db.cursor() as curs:
            curs.execute(query, (filename,))
            return filename, curs.rowcount


def send_notification(job_id, timeout=False):
    """
    If notify is False (the default),this function sends ASVO a POST request saying that the given job ID has
    had all its files staged succesfully.

    If timeout is True, notify ASVO that the job has failed.

    :param job_id: Integer Job ID
    :param timeout: boolean, True if the job has failed (exceeded timeout)
    :return: True if the request succeeded, False otherwise
    """
    post_data = {'job_id':job_id}
    if timeout:
        post_data['ok'] = False
    else:
        post_data['ok'] = True

    result = requests.post(ASVO_URL, data=json.dumps(post_data))
    return result.status_code == 200


def HandleMessages(consumer):
    """
    Runs forever, processing Kafka messages one by one. Exits if there's an exception
    in the message handling code.

    :return:
    """
    msgdb = psycopg2.connect(user=CP['default']['dbuser'],
                             password=CP['default']['dbpass'],
                             host=CP['default']['dbhost'],
                             database=CP['default']['dbname'])
    for msg in consumer:
        try:
            filename, rowcount = process_message(msg, msgdb)
            if rowcount:
                print('File %s staged, updated %d rows in files table' % (filename, rowcount))
            else:
                print('Unknown file: %s' % filename)
        except:
            print(traceback.format_exc())
            return
        finally:
            consumer.commit()   # Tell the Kafka server we've processed that message, so we don't see it again.


def MonitorJobs():
    """
    Runs continously, keeping track of job progress, and notifying ASVO as jobs are completed, or
    as they time out before completion.

    Does not return.
    :return:
    """
    notify_attempts = {}   # Dict with job_id as the key, and unix timestamp as the value for the last attempt
    mondb = psycopg2.connect(user=CP['default']['dbuser'],
                             password=CP['default']['dbpass'],
                             host=CP['default']['dbhost'],
                             database=CP['default']['dbname'])
    while True:
        with mondb:
            with mondb.cursor() as curs:
                curs.execute(COMPLETION_QUERY)
                rows = curs.fetchall()

                # Loop over all jobs not marked as 'notified' (meaning ASVO has been told that it's either completed or failed)
                for job_id, num_files, total_files, completed in rows:
                    # If we now have all the files, mark it as complete
                    if (num_files == total_files) and (not completed):
                        curs.execute('UPDATE staging_jobs SET completed=true WHERE job_id=%s' % (job_id,))
                        completed = True
                    # If it's complete, and it's been more than 10 minutes since we last tried notifing ASVO:
                    if completed and ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                        ok = send_notification(job_id)
                        print("OK=%s" % ok)
                        if ok:
                            curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s' % (job_id,))
                            if job_id in notify_attempts:
                                del notify_attempts[job_id]
                        else:
                            notify_attempts[job_id] = time.time()
                mondb.commit()

                # Loop over all uncompleted jobs, to see if any have timed-out and need ASVO notified about the error
                curs.execute("SELECT job_id, created FROM staging_jobs WHERE NOT completed")
                rows = curs.fetchall()
                for job_id, created in rows:
                    if (datetime.datetime.now(timezone.utc) - created).seconds > EXPIRY_TIME:
                        ok = send_notification(job_id, timeout=True)
                        print("OK=%s" % ok)
                        if ok:
                            curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s' % (job_id,))
                            if job_id in notify_attempts:
                                del notify_attempts[job_id]
                        else:
                            notify_attempts[job_id] = time.time()
                mondb.commit()

                # TODO - add a query to check for and delete the job (and files) for old (already notified) jobs

                time.sleep(CHECK_INTERVAL)  # Check job status at regular intervals


if __name__ == '__main__':
    consumer = KafkaConsumer(MWA_TOPIC,
                             bootstrap_servers=KAFKA_SERVERS,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             group_id='mwa_staging',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # Start the thread that monitors job state and sends completion notifications as necessary
    jobthread = threading.Thread(target=MonitorJobs, name='MonitorJobs')
    jobthread.daemon = True  # Stop this thread when the main program exits.
    jobthread.start()

    # Start processing Kafka messages
    HandleMessages(consumer)  # Never exits.
