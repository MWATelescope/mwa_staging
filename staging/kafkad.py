"""
Daemon to run continuously, receiving Kafka messages about files that have been staged.

Updates the 'staging_jobs' and 'files' tables in the database, and pushes a notification to ASVO
when all files in a job have been staged.

To test with manually generated Kafka messages, eg:

>>> from kafka import KafkaProducer
>>> import json
>>> p = KafkaProducer(bootstrap_servers=['scoutam.pawsey.org.au:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
>>> p.send('mwa', {'Filename':'gibber'})
>>> p.send('mwa', {'Filename':'foo'})

To view messages, do:

from kafka import KafkaConsumer
c = KafkaConsumer('mwa', bootstrap_servers=['scoutam.pawsey.org.au:9092'], auto_offset_reset='earliest', enable_auto_commit=False, group_id='mwa_staging')
c.topics()

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
from datetime import timezone
import glob
import logging
from logging import handlers
import ssl
import subprocess
import threading
import time
import traceback
import urllib3
from urllib3.exceptions import InsecureRequestWarning

import psycopg2
import requests
from requests.auth import AuthBase
from kafka import errors, KafkaConsumer


class KafkadConfig():
    """Config class, used to load configuration data from environment variables.
    """
    def __init__(self):
        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
        self.KAFKA_SERVER = os.getenv('KAFKA_SERVER')
        self.KAFKA_USER = os.getenv('KAFKA_USER')
        self.KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')

        self.SCOUT_LOGIN_URL = os.getenv('SCOUT_LOGIN_URL')
        self.SCOUT_API_USER = os.getenv('SCOUT_API_USER')
        self.SCOUT_API_PASSWORD = os.getenv('SCOUT_API_PASSWORD')
        self.SCOUT_QUERY_URL = os.getenv('SCOUT_QUERY_URL')
        self.SCOUT_STAGE_URL = os.getenv('SCOUT_STAGE_URL')

        self.STAGING_LOGDIR = os.getenv('STAGING_LOGDIR', '/tmp')
        self.REPORT_EXPIREDAYS = os.getenv('REPORT_EXPIREDAYS', 30)

        self.DBUSER = os.getenv('DBUSER')
        self.DBPASSWORD = os.getenv('DBPASSWORD')
        self.DBHOST = os.getenv('DBHOST')
        self.DBNAME = os.getenv('DBNAME')

        self.RESULT_USERNAME = os.getenv('RESULT_USERNAME')
        self.RESULT_PASSWORD = os.getenv('RESULT_PASSWORD')

        self.FILE_RESTAGE_INTERVAL = int(os.getenv('FILE_RESTAGE_INTERVAL', '21600'))   # Repeat file staging request if a job isn't complete.
        self.JOB_EXPIRY_TIME = int(os.getenv('JOB_EXPIRY_TIME', '172800'))


config = KafkadConfig()

LOGLEVEL_LOGFILE = logging.DEBUG   # All messages will be sent to the log file
LOGLEVEL_CONSOLE = logging.INFO    # INFO and above will be printed to STDOUT as well as the logfile
LOGFILE = os.path.join(config.STAGING_LOGDIR, "kafkad.log")

LOGGER = logging.getLogger('kafkad')
LOGGER.setLevel(logging.DEBUG)     # Overridden by the log levels in the file and console handler, if they are less permissive

fh = handlers.RotatingFileHandler(LOGFILE, maxBytes=100000000, backupCount=5)  # 100 Mb per file, max of five old log files
fh.setLevel(LOGLEVEL_LOGFILE)
fh.setFormatter(logging.Formatter(fmt='[%(asctime)s %(levelname)s] %(message)s'))
LOGGER.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL_CONSOLE)
ch.setFormatter(logging.Formatter(fmt='[%(asctime)s %(levelname)s] %(message)s'))
LOGGER.addHandler(ch)

# noinspection PyUnresolvedReferences
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

CHECK_INTERVAL = 60        # Check all job status details once every minute.
RETRY_INTERVAL = 600       # Re-try notifying ASVO about completed jobs every 10 minutes until we succeed

# All jobs that haven't been notified as finished, that have at least one 'ready' file
COMPLETION_QUERY = """
SELECT files.job_id, count(*), staging_jobs.total_files, staging_jobs.completed,
       staging_jobs.notify_url
FROM files JOIN staging_jobs USING(job_id)
WHERE (ready or error) 
GROUP BY (files.job_id, staging_jobs.total_files, staging_jobs.completed,
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


# When the most recent valid Kafka file status message was processed
LAST_KAFKA_MESSAGE = datetime.datetime.utcnow()

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


def get_scout_token(refresh: bool = False):
    """
    Pass the Scout username/password to the SCOUT_LOGIN_URL, and return a token to use for
    subsequent queries.

    :param refresh: boolean, if True, the cached value of the token won't be used.
    :return: token string
    """
    global SCOUT_API_TOKEN
    if SCOUT_API_TOKEN and (not refresh):
        return SCOUT_API_TOKEN
    else:
        data = {'acct':config.SCOUT_API_USER,
                'pass':config.SCOUT_API_PASSWORD}
        result = requests.post(config.SCOUT_LOGIN_URL, json=data, verify=False)
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
    try:
        LOGGER.info("Sending result for job %d to URL %s as user %s" % (job_id, notify_url, config.RESULT_USERNAME))
        result = requests.post(notify_url, json=data, auth=(config.RESULT_USERNAME, config.RESULT_PASSWORD), verify=False)
    except requests.Timeout:
        LOGGER.error('Timout for job %d calling notify_url: %s' % (job_id, notify_url))
        return None
    except ConnectionRefusedError:
        LOGGER.error('Connection refused for job %d calling notify_url %s' % (job_id, notify_url))
        return
    except urllib3.exceptions.NewConnectionError:
        LOGGER.error('"urllib3.exceptions.NewConnectionError" exception for job %d calling notify_url %s' % (job_id, notify_url))
        return
    except urllib3.exceptions.MaxRetryError:
        LOGGER.error('"urllib3.exceptions.MaxRetryError" exception for job %d calling notify_url %s' % (job_id, notify_url))
        return
    except requests.exceptions.ConnectionError:
        LOGGER.error('"requests.exceptions.ConnectionError" exception for job %d calling notify_url %s' % (job_id, notify_url))
        return
    except:
        LOGGER.error('Exception for job %d calling notify_url %s: %s' % (job_id, notify_url, traceback.format_exc()))
        return

    if result.status_code != 200:
        LOGGER.error('Error %d returned for job %d when calling notify_url: %s' % (result.status_code, job_id, result.text))
    else:
        return True


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
    filename = msg.value.get('Filename', None)
    errors = msg.value.get('Error', '')
    if not filename:
        LOGGER.error("Invalid Kafka message, no 'Filename': %s" % msg.value)
        return '', 0, ''

    if not errors:
        # If a file (in another job) was already 'ready', don't change the readytime value
        query = "UPDATE files SET ready=true, error=false, readytime=now() WHERE filename=%s and not ready"
        with db:
            with db.cursor() as curs:
                curs.execute(query, (filename,))
                return filename, curs.rowcount, errors
    else:
        # If a file (in another job) was already 'error', don't change the readytime value
        query = "UPDATE files SET error=true, ready=false, readytime=now() WHERE filename=%s and not error"
        with db:
            with db.cursor() as curs:
                curs.execute(query, (filename,))
                return filename, curs.rowcount, errors


def is_file_ready(filename):
    """
    Ask the Scout API for the status of this file, and return True if 'offlineblocks' is not equal to 0.

    :param filename: File name to query
    :return bool: True if the file is staged and ready.
    """
    result = requests.get(config.SCOUT_QUERY_URL, params={'path':filename}, auth=ScoutAuth(get_scout_token()), verify=False)
    if result.status_code == 403:
        result = requests.get(config.SCOUT_QUERY_URL, params={'path': filename}, auth=ScoutAuth(get_scout_token(refresh=False)), verify=False)
    resdict = result.json()
    offlineblocks = resdict.get('offlineblocks', None)
    if offlineblocks is None:
        LOGGER.error("Unexpected status return from Scout API for file %s: %s" % (filename, resdict))
        return None
    else:
        LOGGER.debug('Got status for file %s: Ready=%s' % (filename, int(offlineblocks) == 0))
        return int(offlineblocks) == 0


def HandleMessages(consumer):
    """
    Runs forever, processing Kafka messages one by one. Exits if there's an exception
    in the message handling code.

    :return:
    """
    global LAST_KAFKA_MESSAGE
    msgdb = psycopg2.connect(user=config.DBUSER,
                             password=config.DBPASSWORD,
                             host=config.DBHOST,
                             database=config.DBNAME)
    for msg in consumer:
        try:
            filename, rowcount, processing_errors = process_message(msg, msgdb)
            if rowcount:
                if processing_errors:
                    LOGGER.warning('File %s error: %s. Updated %d rows in files table' % (filename, processing_errors, rowcount))
                else:
                    LOGGER.info('File %s staged, updated %d rows in files table' % (filename, rowcount))
                LAST_KAFKA_MESSAGE = datetime.datetime.utcnow()
            else:
                LOGGER.warning('Unexpected filename message received: %s' % filename)
        except:
            LOGGER.error(traceback.format_exc())
            LOGGER.error('Exiting!')
            return
        else:
            try:
                consumer.commit()   # Tell the Kafka server we've processed that message, so we don't see it again.
            except errors.CommitFailedError:
                LOGGER.error(traceback.format_exc())
                LOGGER.error('Continuing.')


def job_failed(curs, job_id, total_files):
    """
    Write a file to STAGING_LOGDIR/failed/ with the name '<job_id>.txt' containing a list of all files that weren't
    transferred before the job timed out, and a list of all files with errors.

    :param curs: Psycopg2 database cursor object
    :param job_id: Integer job ID
    :param total_files: Number of files in the job, in total
    :return: True if the write succeeded, False otherwise.
    """
    LOGGER.debug('Called job_failed for job %d' % job_id)
    outdir = os.path.join(config.STAGING_LOGDIR, 'failed')
    if not os.path.isdir(outdir):    # Failed job directory doesn't exist
        LOGGER.error('Failed jobs directory %s does not exist, not writing report for job %d.' % (outdir, job_id))
        return False

    out_filename = os.path.join(outdir, '%d.txt' % job_id)
    if os.path.exists(out_filename):   # File already exists
        LOGGER.error('Job report %s already exists, not re-writing it.')
        return False

    # Clear out old job report files:
    flist = glob.glob(os.path.join(outdir, '*.txt'))
    flist.sort()
    # TODO - check that job IDs always increment!
    deletelist = flist[:-100]  # Never expire the most recent 100 files (assuming job IDs get larger over time)
    for fname in deletelist:
        age = time.time() - os.stat(fname).st_mtime
        if age > float(config.REPORT_EXPIREDAYS) * 86400:
            os.remove(fname)
            LOGGER.info('Deleted old failed job report: %s' % fname)

    curs.execute('SELECT filename from files where job_id=%s and not ready', (job_id,))
    not_ready_files = curs.fetchall()

    curs.execute('SELECT filename from files where job_id=%s and error', (job_id,))
    error_files = curs.fetchall()

    f = open(out_filename, 'w')
    msg = 'Job failure report for job %d: Out of a total of %d files, %d had errors and %d failed to transfer.'
    LOGGER.info(msg % (job_id, total_files, len(error_files), len(not_ready_files)))
    f.write(msg % (job_id, total_files, len(error_files), len(not_ready_files)))
    f.write('\n\n')

    if error_files:
        f.write('Files with errors:\n')
        for row in error_files:
            f.write('  ' + row[0] + '\n')
        f.write('\n')

    if not_ready_files:
        f.write('Files that failed to transfer:\n')
        for row in not_ready_files:
            f.write('  ' + row[0] + '\n')
        f.write('\n')

    f.close()
    return True


def notify_and_delete_job(db, job_id, force_delete=False):
    """
    Call the notify_url, and if that call is successful, delete the job and return True.

    If unsuccessful, return False.

    :param db:  Psycopg2 database object
    :param job_id: integer job ID
    :param force_delete: If True, delete the job even if the notify_url call failed
    :return:
    """
    with db.cursor() as curs:
        try:
            curs.execute('SELECT count(*) from files where job_id=%s and ready', (job_id,))
            ready_files = curs.fetchall()[0][0]

            curs.execute('SELECT count(*) from files where job_id=%s and error', (job_id,))
            error_files = curs.fetchall()[0][0]

            curs.execute('SELECT total_files, notify_url from staging_jobs where job_id=%s', (job_id,))
            total_files, notify_url = curs.fetchall()[0]
        except:
            LOGGER.error('Exception getting file counts - maybe the job was deleted: %s' % traceback.format_exc())
            return False

        if total_files == ready_files:
            return_code = JOB_SUCCESS
            comment = 'All %d files staged successfully' % total_files
            LOGGER.info('Notify call for job %d: All %d files staged succesfully' % (job_id, total_files))
        elif total_files == (ready_files + error_files):
            return_code = JOB_FILE_ERRORS
            comment = 'Out of %d files in total, %d were staged successfully, but %d files had errors' % (total_files,
                                                                                                          ready_files,
                                                                                                          error_files)
            LOGGER.info('Notify call for job %d: Out of %d files, %d were staged, but %d files had errors' % (job_id,
                                                                                                              total_files,
                                                                                                              ready_files,
                                                                                                              error_files))
        else:
            return_code = JOB_TIMEOUT
            comment = 'Job timed out after %d seconds. Out of %d files in total, %d staged successfully, and %d files had errors' % (config.JOB_EXPIRY_TIME,
                                                                                                                                     total_files,
                                                                                                                                     ready_files,
                                                                                                                                     error_files)
            LOGGER.info('Notify call for job %d: Job timed out after %d seconds. Out of %d files, %d were staged, but %d files had errors' % (job_id,
                                                                                                                                              config.JOB_EXPIRY_TIME,
                                                                                                                                              total_files,
                                                                                                                                              ready_files,
                                                                                                                                              error_files))

        ok = send_result(notify_url,
                         job_id,
                         return_code=return_code,
                         total_files=total_files,
                         ready_files=ready_files,
                         error_files=error_files,
                         comment=comment)
        if ok:
            LOGGER.info('Job %d notified.' % job_id)
        else:
            LOGGER.error('Job %d failed to notify.' % job_id)
            if not force_delete:
                return False

        if return_code != JOB_SUCCESS:   # Create a file in the failed jobs directory, listing the files that failed:
            job_failed(curs=curs, job_id=job_id, total_files=total_files)

        try:
            curs.execute("DELETE FROM files WHERE job_id = %s", (job_id,))
            curs.execute("DELETE FROM staging_jobs WHERE job_id = %s", (job_id,))
            LOGGER.info('Job %d DELETED.' % job_id)
            return True
        except:
            LOGGER.error('Exception when deleting job %d: %s' % (job_id, traceback.format_exc()))
            return False


def restage_job(curs, job_id):
    """
    Ask Scout to stage all the files in this job, in case the original staging request was lost.

    :param curs:   Psycopg2 cursor object
    :param job_id: integer job ID
    :return: True if Scout API called successfully.
    """
    pathlist = []
    try:
        curs.execute('SELECT filename from files where job_id=%s and not (ready or error)', (job_id,))
        rows = curs.fetchall()
        for row in rows:
            pathlist.append(row[0])

        ok = True
        status_code, result_text = 0, ''
        # Split filenames into groups of 10000, to avoid a request size > 4 MB
        for fgroup in range(1 + int(len(pathlist)) // 10000):
            grouplist = pathlist[fgroup * 10000:fgroup * 10000 + 10000]
            data = {'path': grouplist, 'copy': 0, 'inode': [], 'key': str(job_id), 'topic': config.KAFKA_TOPIC}
            result = requests.put(config.SCOUT_STAGE_URL,
                                  json=data,
                                  auth=ScoutAuth(get_scout_token()),
                                  verify=False)
            if result.status_code == 403:
                result = requests.put(config.SCOUT_STAGE_URL,
                                      json=data,
                                      auth=ScoutAuth(get_scout_token(refresh=True)),
                                      verify=False)
            status_code, result_text = result.status_code, result.text
            LOGGER.debug('Got result for staging call %d for job %d from Scout API batchstage call: %d:%s' % (fgroup + 1, job_id, status_code, result_text))

            if status_code != 200:  # Error requesting files to be staged
                LOGGER.error('Got result code of %d from Scout re-stage request: %s' % (result.status_code, result.text))
                ok = False

        if ok:  # All files requested to be staged
            return True
        else:
            LOGGER.error('Got result code of %d from Scout re-stage request: %s' % (status_code, result_text))
            return False
    except (ssl.SSLEOFError, urllib3.exceptions.MaxRetryError, requests.exceptions.SSLError):  # Scout server not available
        LOGGER.error('Scout server not responding to %s in restage_job' % config.SCOUT_STAGE_URL)
        return False
    except:
        LOGGER.error('Exception when re-staging files for job %d: %s' % (job_id, traceback.format_exc()))
        return False


def MonitorJobs(consumer):
    """
    Runs continously, keeping track of job progress, and notifying ASVO as jobs are completed, or
    as they time out before completion.

    Does not return.
    :return:
    """
    notify_attempts = {}    # Dict with job_id as the key, and unix timestamp as the value for the last attempt
    restage_attempts = {}   # Dict with job_id as the key, and unix timestamp for the last time a 're-stage files' call
                            # was made as the value
    mondb = psycopg2.connect(user=config.DBUSER,
                             password=config.DBPASSWORD,
                             host=config.DBHOST,
                             database=config.DBNAME)
    LOGGER.info('Starting MonitorJobs thread.')
    while True:
        with mondb:
            with mondb.cursor() as curs:
                curs.execute(COMPLETION_QUERY)
                rows = curs.fetchall()

                LOGGER.debug('Checking to see if jobs are complete')
                # Loop over all jobs that haven't been notified as finished, that have at least one 'ready' or 'error' file
                for job_id, num_files, total_files, completed, notify_url in rows:
                    LOGGER.debug('Check completion on job %d' % job_id)
                    if completed:   # Already marked as complete, but ASVO hasn't been successfully notified:
                        if ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                            ok = notify_and_delete_job(db=mondb, job_id=job_id)
                            if ok:
                                mondb.commit()
                                if job_id in notify_attempts:
                                    del notify_attempts[job_id]
                                if job_id in restage_attempts:
                                    del restage_attempts[job_id]
                            else:
                                notify_attempts[job_id] = time.time()
                    else:
                        if (num_files == total_files):  # If all the files are either 'ready' or 'error', mark it as complete
                            LOGGER.info('    job %d marked as complete.' % job_id)
                            curs.execute('UPDATE staging_jobs SET completed=true WHERE job_id=%s', (job_id,))
                            mondb.commit()
                            if ((time.time() - notify_attempts.get(job_id, 0)) > RETRY_INTERVAL):
                                ok = notify_and_delete_job(db=mondb, job_id=job_id)
                                if ok:
                                    mondb.commit()
                                    if job_id in notify_attempts:
                                        del notify_attempts[job_id]
                                    if job_id in restage_attempts:
                                        del restage_attempts[job_id]
                                else:
                                    notify_attempts[job_id] = time.time()
                        else:
                            LOGGER.info('    job %d has num_files=%d, total_files=%d, completed=%s' % (job_id, num_files, total_files, completed))

                mondb.commit()

                LOGGER.debug('Check to see if any jobs need re-staging, or have timed out while waiting for completion')
                # Loop over all uncompleted jobs, to see if any need re-staging, or have timed and the client should be notified about the error
                curs.execute("SELECT job_id, created, notify_url, completed FROM staging_jobs")
                rows = curs.fetchall()
                for job_id, created, notify_url, completed in rows:
                    LOGGER.debug('Checking job %d' % job_id)
                    job_age = (datetime.datetime.now(timezone.utc) - created).total_seconds()
                    last_stage = min((time.time() - restage_attempts.get(job_id, 0)), job_age)
                    if completed:
                        LOGGER.debug("Completed Job %d is %d seconds old, and was last staged %d seconds ago." % (job_id,
                                                                                                                  job_age,
                                                                                                                  last_stage))
                    else:
                        LOGGER.debug('Incomplete Job %d is %d seconds old, and was last staged %d seconds ago.' % (job_id,
                                                                                                                   job_age,
                                                                                                                   last_stage))
                    if job_age > config.JOB_EXPIRY_TIME:
                        ok = notify_and_delete_job(db=mondb, job_id=job_id, force_delete=True)
                        LOGGER.debug('Returned from deleting job %d.' % job_id)
                        if ok:
                            LOGGER.debug('ok is True')
                            if job_id in notify_attempts:
                                LOGGER.debug('job_id %d is in notify_attempts' % job_id)
                                del notify_attempts[job_id]
                                LOGGER.debug('del notify_attempts[job_id] succeeded')
                            else:
                                LOGGER.debug('job_id %d is not in notify_attempts' % job_id)

                            if job_id in restage_attempts:
                                LOGGER.debug('job_id %d is in restage_attempts' % job_id)
                                del restage_attempts[job_id]
                                LOGGER.debug('del restage_attempts[job_id] succeeded')
                            else:
                                LOGGER.debug('job_id %d is not in restage_attempts' % job_id)
                        else:
                            LOGGER.debug('ok is False')
                            notify_attempts[job_id] = time.time()
                        LOGGER.debug('Job %d expired' % job_id)
                    elif (not completed) and (last_stage > config.FILE_RESTAGE_INTERVAL):
                        LOGGER.info('Restaging job %d' % job_id)
                        ok = restage_job(curs=curs, job_id=job_id)
                        if ok:
                            restage_attempts[job_id] = time.time()
                            LOGGER.info('Restaging job %d succeeded' % job_id)
                        else:
                            LOGGER.info('Restaging job %d failed' % job_id)

                LOGGER.debug('Committing:')
                mondb.commit()

                LOGGER.debug('Check to see if the Kafka daemon is still talking to us')
                try:
                    consumer.topics()   # Make sure the remote end responds
                    connection_alive = True
                except errors.KafkaError:
                    connection_alive = False
                LOGGER.debug('Updating heartbeat table')
                curs.execute(UPDATE_HEARTBEAT_QUERY, (connection_alive, LAST_KAFKA_MESSAGE))
                mondb.commit()

                LOGGER.debug('Sleeping')
                time.sleep(CHECK_INTERVAL)  # Check job status at regular intervals


if __name__ == '__main__':
    hostname = subprocess.check_output(args=['hostname'], shell=False).decode('utf8').strip()
    LOGGER.info('Starting kafkad main thread.')
    while True:
        try:
            ssl_settings = ssl.SSLContext(ssl.PROTOCOL_TLS)
            ssl_settings.verify_mode = ssl.CERT_NONE

            consumer = KafkaConsumer(config.KAFKA_TOPIC,
                                     bootstrap_servers=[config.KAFKA_SERVER],
                                     auto_offset_reset='earliest',
                                     enable_auto_commit=False,
                                     client_id=hostname,
                                     group_id='mwagroup',
                                     sasl_mechanism='SCRAM-SHA-512',
                                     sasl_plain_username=config.KAFKA_USER,
                                     sasl_plain_password=config.KAFKA_PASSWORD,
                                     security_protocol='SASL_SSL',
                                     ssl_context=ssl_settings,
                                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            LOGGER.info('Connected to Kafka server.')
            # Start the thread that monitors job state and sends completion notifications as necessary
            jobthread = threading.Thread(target=MonitorJobs, name='MonitorJobs', args=(consumer,))
            jobthread.daemon = True  # Stop this thread when the main program exits.
            jobthread.start()

            # Start processing Kafka messages
            HandleMessages(consumer)  # Never exits.
        except errors.NoBrokersAvailable as e:
            LOGGER.error('Unable to connect to Kafka server.')
            LOGGER.error(e)

        LOGGER.info('Sleeping for 5 minutes, waiting for the Kafka server to come back.')
        time.sleep(300)
