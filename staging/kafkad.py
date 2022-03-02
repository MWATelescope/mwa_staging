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

From Harrison:
I’ve setup rclone as the ubuntu user and added the test vm as a remote called test. Just type rclone and it gives you the help menu for how to use it, it’s got a really nice CLI

Examples:
rclone ls test: - list all buckets and files
rclone mkdir test:12345 - create a bucket called 12345 in the test remote
rclone copy test.txt test:12345 - copy test.txt to the bucket created above

and

Sample Kafka messages:
{"Inode":45871,"Filename":"nfs/file1","RequestTime":"2022-02-04 03:01:38 +0000 GMT","CompleteTime":"2022-02-04 03:02:32 +0000 GMT","Error":""}
{"Inode":45871,"Filename":"nfs/file1","RequestTime":"2022-02-04 03:00:38 +0000 GMT","CompleteTime":"2022-02-04 03:00:53 +0000 GMT","Error":"unable to stage file, no valid copies"}

An empty error string indicates no error on the stage.
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
SELECT files.job_id, count(*), staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked,
       staging_jobs.notify_url
FROM files JOIN staging_jobs USING(job_id)
WHERE (ready or error) AND (not staging_jobs.notified) 
GROUP BY (files.job_id, staging_jobs.total_files, staging_jobs.completed, staging_jobs.checked,
       staging_jobs.notify_url)
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
    """Config class, used to load configuration data from environment variables.
    """
    def __init__(self):
        self.MWA_TOPIC = os.getenv('MWA_TOPIC')  # Kafka topic to listen on
        self.KAFKA_SERVER = os.getenv('KAFKA_SERVER')

        self.SCOUT_LOGIN_URL = os.getenv('SCOUT_LOGIN_URL')
        self.SCOUT_API_USER = os.getenv('SCOUT_API_USER')
        self.SCOUT_API_PASSWORD = os.getenv('SCOUT_API_PASSWORD')
        self.SCOUT_QUERY_URL = os.getenv('SCOUT_QUERY_URL')

        self.DBUSER = os.getenv('DBUSER')
        self.DBPASSWORD = os.getenv('DBPASSWORD')
        self.DBHOST = os.getenv('DBHOST')
        self.DBNAME = os.getenv('DBNAME')

        self.RESULT_USERNAME = os.getenv('RESULT_USERNAME')
        self.RESULT_PASSWORD = os.getenv('RESULT_PASSWORD')


config = KafkadConfig()

# When the most recent valid Kafka file status message was processed
LAST_KAFKA_MESSAGE = None

SCOUT_API_TOKEN = ''

# Job return codes:

JOB_SUCCESS = 0                # All files staged successfully
JOB_FILE_ERRORS = 5            # All files either ready, or had errors from Kafka
JOB_TIMEOUT = 10               # Timeout waiting for all files to stage - comment field will contain number of staged and outstanding files
JOB_FILE_LOOKUP_FAILED = 100   # Failed to look up files associated with the given observation (eg failure in metadata/data_files web service call)
JOB_NO_FILES = 101             # No files to stage
JOB_SCOUT_CALL_FAILED = 102    # Failure in call to Scout API to stage files
JOB_CREATION_EXCEPTION = 199   # An exception occurred while creating the job. Comment field will contain exception traceback


class ScoutAuth(AuthBase):
    """Attaches the 'Authorization: Bearer $TOKEN' header to the request, for authenticating Scout API
       requests, in calls to the requests module.
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


def get_scout_token():
    """
    Pass the Scout username/password to the SCOUT_LOGIN_URL, and return a token to use for
    subsequent queries.
    :return: token string
    """
    global SCOUT_API_TOKEN
    if SCOUT_API_TOKEN:
        return SCOUT_API_TOKEN
    else:
        data = {'acct':config.SCOUT_API_USER,
                'pass':config.SCOUT_API_PASSWORD}
        result = requests.post(config.SCOUT_LOGIN_URL, json=data)
        if result.status_code == 200:
            SCOUT_API_TOKEN = result.json()['response']
            return SCOUT_API_TOKEN


def send_result(notify_url,
                job_id,
                return_code=0,
                total_files=0,
                ready_files=0,
                error_files=0,
                comment=''):
    """
    Call the given URL to report the success or failure of a job, passing a JobResult structure

    :param notify_url: URL to send job result to
    :param job_id: Integer job ID
    :param return_code: Integer return code, eg JOB_SUCCESS=0
    :param total_files: Total number of files in job
    :param ready_files: Number of files successfully staged
    :param error_files: Number of files where Kafka returned an error
    :param comment: Human readable string (eg error messages)
    :return:
    """
    data = {'job_id':job_id,
            'return_code':return_code,
            'total_files':total_files,
            'ready_files':ready_files,
            'error_files':error_files,
            'comment':comment}
    result = requests.post(notify_url, json=data, auth=(config.RESULT_USERNAME, config.RESULT_PASSWORD))
    return result.status_code == 200


def process_message(msg, db):
    """
    Handle a single message from Kafka, when a new file has been staged. Here msg.value has already
    been de-serialised, so it's a Python dictionary, with contents TBD.

    For development, msg.value['Filename'] will contain the path+filename of the file that has
    just been staged, and msg.value['Error'] will be an empty string if the file was staged with
    no errors, or an error message if the file could not be staged. For example:

    {"Inode":45871,
     "Filename":"nfs/file1",
     "RequestTime":"2022-02-04 03:01:38 +0000 GMT",
     "CompleteTime":"2022-02-04 03:02:32 +0000 GMT",
     "Error":""}

    or

    {"Inode":45871,
     "Filename":"nfs/file1",
     "RequestTime":"2022-02-04 03:00:38 +0000 GMT",
     "CompleteTime":"2022-02-04 03:00:53 +0000 GMT",
     "Error":"unable to stage file, no valid copies"}

    :param msg: Named Tuple from Kafka, with attributes 'topic', 'partition', 'offset', 'key' and 'value'
    :param db: Database connection object
    :return: Number of rows updated in the files table
    """
    global LAST_KAFKA_MESSAGE
    filename = msg.value.get('Filename', None)
    errors = msg.value.get('Error', '')
    if filename is None:
        print("Invalid message, no 'filename' key: %s" % msg.value)
        return '', 0

    LAST_KAFKA_MESSAGE = datetime.datetime.utcnow()

    if not errors:
        # If a file (in another job) was already 'ready', don't change the readytime value
        query = "UPDATE files SET ready=true, readytime=now() WHERE filename=%s and not ready"
        with db:
            with db.cursor() as curs:
                curs.execute(query, (filename,))
                return filename, curs.rowcount
    else:
        # If a file (in another job) was already 'ready', don't change the readytime value
        query = "UPDATE files SET error=true, readytime=now() WHERE filename=%s and not ready"
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
    result = requests.get(config.SCOUT_QUERY_URL, params={'path':filename}, auth=ScoutAuth(get_scout_token()))
    resdict = result.json()
    print('Got status for file %s: %s' % (filename, resdict))
    return resdict['offlineblocks'] == 0


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


def notify_job(db, job_id):
    """

    :param db:
    :param job_id:
    :return:
    """
    with db:
        with db.cursor() as curs:
            curs.execute('SELECT count(*) from files where job_id=%s and ready', (job_id,))
            ready_files = curs.fetchall()[0][0]

            curs.execute('SELECT count(*) from files where job_id=%s and error', (job_id,))
            error_files = curs.fetchall()[0][0]

            curs.execute('SELECT total_files, notify_url from staging_jobs where job_id=%s', (job_id,))
            total_files, notify_url = curs.fetchall()[0]

    if total_files == ready_files:
        return_code = JOB_SUCCESS
        comment = 'All %d files staged successfully' % total_files
    elif total_files == (ready_files + error_files):
        return_code = JOB_FILE_ERRORS
        comment = 'Out of %d files in total, %d were staged successfully, but %d files had errors' % (total_files,
                                                                                                      ready_files,
                                                                                                      error_files)
    else:
        return_code = JOB_TIMEOUT
        comment = 'Job timed out after %d seconds. Out of %d files in total, %d staged successfully, and %d files had errors' % (EXPIRY_TIME,
                                                                                                                                 total_files,
                                                                                                                                 ready_files,
                                                                                                                                 error_files)

    ok = send_result(notify_url,
                     job_id,
                     return_code=return_code,
                     total_files=total_files,
                     ready_files=ready_files,
                     error_files=error_files,
                     comment=comment)
    return ok   # True if the call to notify ASVO was successful


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
                # Loop over all jobs that haven't been notified as finished, that have at least one 'ready' or 'error' file
                for job_id, num_files, total_files, completed, checked in rows:
                    print('Check completion on job %d' % job_id)
                    if completed:   # Already marked as complete, but ASVO hasn't been successfully notified:
                        if ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                            ok = notify_job(db=mondb, job_id=job_id)
                            if ok:
                                curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s', (job_id,))
                                if job_id in notify_attempts:
                                    del notify_attempts[job_id]
                            else:
                                notify_attempts[job_id] = time.time()
                    else:
                        if (num_files == total_files):  # If all the files are either 'ready' or 'error', mark it as complete
                            print('    job %d marked as complete.' % job_id)
                            curs.execute('UPDATE staging_jobs SET completed=true WHERE job_id=%s', (job_id,))
                            if ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                                ok = notify_job(db=mondb, job_id=job_id)
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

                print('Check to see if any jobs have timed out while waiting for completion')
                # Loop over all uncompleted jobs, to see if any have timed-out and the client should be notified about the error
                curs.execute("SELECT job_id, created, notify_url FROM staging_jobs WHERE NOT completed")
                rows = curs.fetchall()
                for job_id, created, notify_url in rows:
                    if (datetime.datetime.now(timezone.utc) - created).seconds > EXPIRY_TIME:
                        ok = notify_job(db=mondb, job_id=job_id)
                        if ok:
                            curs.execute('UPDATE staging_jobs SET notified=true WHERE job_id=%s', (job_id,))
                            if job_id in notify_attempts:
                                del notify_attempts[job_id]
                        else:
                            notify_attempts[job_id] = time.time()
                mondb.commit()
                # TODO - flag job as 'notified' if we've timed out

                # TODO - add check to see if any jobs have files that are all either ready, or error, so none are waiting.

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
