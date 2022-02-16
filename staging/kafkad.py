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

import os
import json
import datetime
import threading
import time
import traceback
import psycopg2
import requests
from requests.auth import AuthBase

from datetime import timezone
from kafka import errors, KafkaConsumer


CHECK_INTERVAL = 60    # Check all job status details once every minute.
RETRY_INTERVAL = 600   # Re-try notifying ASVO about completed jobs every 10 minutes until we succeed
EXPIRY_TIME = 86400    # Return an error if it's been more than a day since a job was staged, and it's still not finished.

# All jobs that haven't been notified as finished, that have at least one 'ready' file
COMPLETION_QUERY = """
SELECT files.job_id, count(*), staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked
FROM files JOIN staging_jobs USING(job_id)
WHERE ready AND (not staging_jobs.notified) 
GROUP BY (files.job_id, staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked)
"""

# Most recent 'readytime' for this given filename, that is NOT from the given job ID.
ALREADY_DONE_QUERY = """
SELECT readytime 
FROM files 
WHERE (filename = %s) AND (job_id <> %s) AND ready 
ORDER BY readytime DESC LIMIT 1
"""

# Update the kafka_heartbeat table with the current status
UPDATE_HEARTBEAT_QUERY = """
UPDATE kafkad_heartbeat 
SET update_time = now(), 
    kafka_alive = %s,
    last_message = %s
WHERE true
"""


class KafkadConfig():
    def __init__(self):
        self.MWA_TOPIC = os.getenv('MWA_TOPIC') # Kafka topic to listen on
        self.KAFKA_SERVER = os.getenv('KAFKA_SERVER')
        self.ASVO_URL = os.getenv('ASVO_URL')
        self.SCOUT_QUERY_URL = os.getenv('SCOUT_QUERY_URL')
        self.SCOUT_API_TOKEN = os.getenv('SCOUT_API_TOKEN')
        self.DBUSER = os.getenv('DBUSER')
        self.DBPASSWORD = os.getenv('DBPASSWORD')
        self.DBHOST = os.getenv('DBHOST')
        self.DBNAME = os.getenv('DBNAME')


config = KafkadConfig()

# When the most recent valid Kafka file status message was processed
LAST_KAFKA_MESSAGE = None



class ScoutAuth(AuthBase):
    """Attaches the 'Authorization: Bearer $TOKEN' header to the request, for authenticating Scout API
       requests.
    """
    def __init__(self, token):
        """
        Store the API token

        :param token: token string from Scout's /v1/security/login endpoint
        """
        self.token = token

    def __call__(self, r):
        """
        Modify the request by adding the Scout token to the header

        :param r: request object
        :return: modified request object
        """
        r.headers['Authorization'] = 'Bearer %s' % self.token
        return r


def process_message(msg, db):
    """
    Handle a single message from Kafka, when a new file has been staged. Here msg.value has already
    been de-serialised, so it's a Python dictionary, with contents TBD.

    :param msg: Named Tuple from Kafka, with attributes 'topic', 'partition', 'offset', 'key' and 'value'
    :param db: Database connection object
    :return: Number of rows updated in the files table
    """
    global LAST_KAFKA_MESSAGE
    filename = msg.value.get('filename', None)
    if filename is None:
        print("Invalid message, no 'filename' key: %s" % msg.value)
        return '', 0

    LAST_KAFKA_MESSAGE = datetime.datetime.utcnow()

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
    result = requests.get(config.SCOUT_QUERY_URL, params={'path':filename}, auth=ScoutAuth(config.SCOUT_API_TOKEN))
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

    result = requests.post(config.ASVO_URL, data=json.dumps(post_data))
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
    with db.cursor() as curs:
        curs.execute('SELECT filename FROM files WHERE job_id=%s AND not ready', (job_id,))
        rows = curs.fetchall()
        file_dict = {}   # Dict with filename as key, and readytime as value
        for row in rows:
            filename = row[0]
            print('    File %s from job %d:' % (filename, job_id))
            # Find the most recently 'ready' file with the same name but from a different job
            curs.execute(ALREADY_DONE_QUERY, (filename, job_id))
            result = curs.fetchall()
            if result:
                print('        Found already done at %s' % result[0][0])
                file_dict[filename] = result[0][0]
            else:
                print('        Not found previously.')

        if file_dict:
            earliest_good = datetime.datetime(year=9999, month=12, day=31, tzinfo=timezone.utc)
            check_files = list(file_dict.keys())
            check_files.sort(key=lambda x:file_dict[x])
            if not is_file_ready(check_files[-1]):   # Check the most recently ready file first
                return   # The most recently ready file is still in the cache, so none of the older ones will be

            for filename in check_files:
                print('    Previous file %s:' % filename)
                readytime = file_dict[filename]
                if file_dict[filename] >= earliest_good:   # This file was ready more recently than one we know is still cached
                    print('        More recent than %s, so marked as ready' % earliest_good)
                    # Update all not-ready records for this file to say that it's still cached, with the old readytime
                    curs.execute('UPDATE files SET ready = true, readytime = %s WHERE filename=%s and not ready',
                                 (readytime, filename))
                else:   # This file is older than one we know is still cached
                    if is_file_ready(filename=filename):   # If it is still cached
                        print('        Is still ready.')
                        # Update all not-ready records for this file to say that it's still cached, with the old readytime
                        curs.execute('UPDATE files SET ready=true, readytime = %s WHERE filename=%s and not ready',
                                     (readytime, filename))
                        earliest_good = file_dict[filename]
                    else:
                        print('        Is not still ready.')


def HandleMessages(consumer):
    """
    Runs forever, processing Kafka messages one by one. Exits if there's an exception
    in the message handling code.

    :return:
    """
    msgdb = psycopg2.connect(user=config.DBUSER,
                             password=config.DBPASSWORD,
                             host=config.DBHOST,
                             database=config.DBNAME)
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


def MonitorJobs(consumer):
    """
    Runs continously, keeping track of job progress, and notifying ASVO as jobs are completed, or
    as they time out before completion.

    Does not return.
    :return:
    """
    notify_attempts = {}   # Dict with job_id as the key, and unix timestamp as the value for the last attempt
    mondb = psycopg2.connect(user=config.DBUSER,
                             password=config.DBPASSWORD,
                             host=config.DBHOST,
                             database=config.DBNAME)
    while True:
        with mondb:
            with mondb.cursor() as curs:
                curs.execute(COMPLETION_QUERY)
                rows = curs.fetchall()

                print('Checking to see if jobs are complete')
                # Loop over all jobs that haven't been notified as finished, that have at least one 'ready' file
                for job_id, num_files, total_files, completed, checked in rows:
                    print('Check completion on job %d' % job_id)
                    # If we now have all the files, mark it as complete
                    if (num_files == total_files):
                        print('    job %d has %d out of %d files' % (job_id, num_files, total_files))
                        if (not completed):
                            print('    job %d marked as complete.' % job_id)
                            curs.execute('UPDATE staging_jobs SET completed=true WHERE job_id=%s', (job_id,))
                            completed = True
                    # If it's complete, and it's been more than 10 minutes since we last tried notifing ASVO:
                    if completed and ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                        ok = send_notification(job_id)
                        print("job %d, OK=%s" % (job_id, ok))
                        if ok:
                            curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s', (job_id,))
                            if job_id in notify_attempts:
                                del notify_attempts[job_id]
                        else:
                            notify_attempts[job_id] = time.time()

                print('Looking to see if any jobs need to be checked for already-staged files')
                curs.execute('SELECT job_id FROM staging_jobs WHERE not notified AND not checked')
                rows = curs.fetchall()
                for row in rows:
                    job_id = row[0]
                    print('Check for already staged files on job %d' % job_id)
                    check_job_for_existing_files(job_id=job_id, db=mondb)
                    curs.execute('UPDATE staging_jobs SET checked=true WHERE job_id=%s', (job_id,))

                mondb.commit()

                print('Check to see if any completed files need ASVO notified again')
                # Loop over all uncompleted jobs, to see if any have timed-out and need ASVO notified about the error
                curs.execute("SELECT job_id, created FROM staging_jobs WHERE NOT completed")
                rows = curs.fetchall()
                for job_id, created in rows:
                    if (datetime.datetime.now(timezone.utc) - created).seconds > EXPIRY_TIME:
                        ok = send_notification(job_id, timeout=True)
                        print("OK=%s" % ok)
                        if ok:
                            curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s', (job_id,))
                            if job_id in notify_attempts:
                                del notify_attempts[job_id]
                        else:
                            notify_attempts[job_id] = time.time()
                mondb.commit()

                print('Check to see if the Kafka daemon is still talking to us')
                try:
                    consumer.topics()   # Make sure the remote end responds
                    connection_alive = True
                except errors.KafkaError:
                    connection_alive = False
                print('Updating heartbeat table')
                curs.execute(UPDATE_HEARTBEAT_QUERY, (connection_alive, LAST_KAFKA_MESSAGE))
                mondb.commit()

                # TODO - add a query to check for and delete the job (and files) for old (already notified) jobs

                print('Sleeping')
                time.sleep(CHECK_INTERVAL)  # Check job status at regular intervals


if __name__ == '__main__':
    consumer = KafkaConsumer(config.MWA_TOPIC,
                             bootstrap_servers=config.KAFKA_SERVER,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             group_id='mwa_staging',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # Start the thread that monitors job state and sends completion notifications as necessary
    jobthread = threading.Thread(target=MonitorJobs, name='MonitorJobs', args=(consumer,))
    jobthread.daemon = True  # Stop this thread when the main program exits.
    jobthread.start()

    # Start processing Kafka messages
    HandleMessages(consumer)  # Never exits.
