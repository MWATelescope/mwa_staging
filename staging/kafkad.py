"""
Daemon to run continuously, receiving Kafka messages about files that have been staged.

Updates the 'staging_jobs' and 'files' tables in the database, and pushes a notification to ASVO
when all files in a job have been staged.

To test with manually generated Kafka messages, eg:

>>> from kafka import KafkaProducer
>>> import json
>>> p = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
>>> p.send('mwa', {'filename':'gibber'})
>>> p.send('mwa', {'filename':'foo'})
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
SCOUT_QUERY_URL = 'http://localhost:8000/v1/file'

CHECK_INTERVAL = 60    # Check all job status details once every minute.
RETRY_INTERVAL = 600   # Re-try notifying ASVO about completed jobs every 10 minutes until we succeed
EXPIRY_TIME = 86400    # Return an error if it's been more than a day since a job was staged, and it's still not finished.

COMPLETION_QUERY = """
SELECT files.job_id, count(*), staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked
FROM files JOIN staging_jobs USING(job_id)
WHERE ready AND (not staging_jobs.notified) 
GROUP BY (files.job_id, staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked)
"""

# All 'ready' files with a given name, that are NOT from the given job ID.
ALREADY_DONE_QUERY = """
SELECT readytime 
FROM files 
WHERE (filename = %s) AND (job_id <> %s) AND ready 
ORDER BY readytime DESC LIMIT 1
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

    # If a file (in another job) was already 'ready', don't change the readytime value
    query = "UPDATE files SET ready=true, readytime=now() WHERE filename=%s and not ready"
    with db:
        with db.cursor() as curs:
            curs.execute(query, (filename,))
            return filename, curs.rowcount


def is_file_ready(filename):
    """
    Ask the Scout API for the status of this file, and return True if 'offlineblocks' is not equal to 0.

    :param filename: File name to query
    :return bool: True if the file is staged and ready.
    """
    result = requests.get(SCOUT_QUERY_URL, params={'pathname':filename})
    resdict = result.json()
    print('Got status for file %s: %s' % (filename, resdict))
    return resdict['offlineblocks'] == 0


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


def check_job_for_existing_files(job_id, db):
    """
    For each of the files in this job, find all the files that were in an already-processed job, and marked as ready
    there. If there are any files with matching names that were staged for a different job in the past, and might still
    be in the cache, query Scout about their status from oldest to newest until we find one that is in the cache. Then
    assume that all newer files will still be cached too, so update their state in the files table.

    :param job_id: Integer Job ID
    :param db: Database connection object
    :return: None
    """
    with db:
        with db.cursor() as curs:
            curs.execute('SELECT filename FROM files WHERE job_id=%s AND not ready', (job_id,))
            rows = curs.fetchall()
            file_dict = {}   # Dict with filename as key, and readytime as value
            for filename in rows:
                # Find the most recently 'ready' file with the same name but from a different job
                curs.execute(ALREADY_DONE_QUERY, (filename, job_id))
                result = curs.fetchall()
                if result:
                    file_dict[filename] = result[0][0]

            if file_dict:
                earliest_good = datetime.datetime(year=9999, month=0, day=0)
                check_files = list(file_dict.keys())
                check_files.sort(key=lambda x:file_dict[x])
                for filename in check_files:
                    readytime = file_dict[filename]
                    if file_dict[filename] >= earliest_good:   # This file was ready more recently than one we know is still cached
                        # Update all not-ready records for this file to say that it's still cached, with the old readytime
                        curs.execute('UPDATE files SET ready = true, readytime = %s WHERE filename=%s and not ready',
                                     (readytime, filename))
                    else:   # This file is older than one we know is still cached
                        if is_file_ready(filename=filename):   # If it is still cached
                            # Update all not-ready records for this file to say that it's still cached, with the old readytime
                            curs.execute('UPDATE files SET ready=true, readytime = %s WHERE filename=%s and not ready',
                                         (readytime, filename))
                            earliest_good = file_dict[filename]


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
                for job_id, num_files, total_files, completed, checked in rows:
                    # If we now have all the files, mark it as complete
                    if (num_files == total_files):
                        if (not completed):
                            curs.execute('UPDATE staging_jobs SET completed=true WHERE job_id=%s' % (job_id,))
                            completed = True
                    elif not checked:  # Check to see if the files in this job were in some previous job, and still cached
                        check_job_for_existing_files(job_id=job_id, db=mondb)
                        curs.execute('UPDATE staging_jobs SET checked=true WHERE job_id=%s' % (job_id,))
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
