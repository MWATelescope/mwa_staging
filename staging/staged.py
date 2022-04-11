"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

import datetime
from datetime import timezone
import os
import logging
from logging import handlers
import traceback

import psycopg2
from psycopg2 import extras

from fastapi import FastAPI, BackgroundTasks, Query, Request, Response, status
import requests
from requests.auth import AuthBase
from urllib3.exceptions import InsecureRequestWarning

from mwa_files import get_mwa_files as get_files
import staged_models as models
import staged_db


# noinspection PyUnresolvedReferences
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

LOGLEVEL_LOGFILE = logging.DEBUG   # All messages will be sent to the log file
LOGLEVEL_CONSOLE = logging.INFO    # INFO and above will be printed to STDOUT as well as the logfile
LOGFILE = "/var/log/staging/staged.log"

LOGGER = logging.getLogger('staged')
LOGGER.setLevel(logging.DEBUG)     # Overridden by the log levels in the file and console handler, if they are less permissive

fh = handlers.RotatingFileHandler(LOGFILE, maxBytes=100000000, backupCount=5)  # 100 Mb per file, max of five old log files
fh.setLevel(LOGLEVEL_LOGFILE)
fh.setFormatter(logging.Formatter(fmt='[%(asctime)s %(levelname)s] %(message)s'))
LOGGER.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL_CONSOLE)
ch.setFormatter(logging.Formatter(fmt='[%(asctime)s %(levelname)s] %(message)s'))
LOGGER.addHandler(ch)


class ApiConfig():
    """Config class, used to load configuration data from environment variables.
    """
    def __init__(self):
        self.DBUSER = os.getenv('DBUSER')
        self.DBPASSWORD = os.getenv('DBPASSWORD')
        self.DBHOST = os.getenv('DBHOST')
        self.DBNAME = os.getenv('DBNAME')

        self.SCOUT_STAGE_URL = os.getenv('SCOUT_STAGE_URL')
        self.SCOUT_CANCEL_URL = os.getenv('SCOUT_CANCEL_URL')
        self.SCOUT_LOGIN_URL = os.getenv('SCOUT_LOGIN_URL')
        self.SCOUT_API_USER = os.getenv('SCOUT_API_USER')
        self.SCOUT_API_PASSWORD = os.getenv('SCOUT_API_PASSWORD')
        self.SCOUT_QUERY_URL = os.getenv('SCOUT_QUERY_URL')
        self.SCOUT_CACHESTATE_URL = os.getenv('SCOUT_CACHESTATE_URL')

        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

        self.RESULT_USERNAME = os.getenv('RESULT_USERNAME')
        self.RESULT_PASSWORD = os.getenv('RESULT_PASSWORD')


config = ApiConfig()

SCOUT_API_TOKEN = ''

# Job return codes:

JOB_SUCCESS = 0                 # All files staged successfully
JOB_TIMEOUT = 10                # Timeout waiting for all files to stage - comment field will contain number of staged and outstanding files
JOB_FILE_LOOKUP_FAILED = 100    # Failed to look up files associated with the given observation (eg failure in metadata/data_files web service call)
JOB_NO_FILES = 101              # No files to stage
# JOB_SCOUT_CALL_FAILED = 102     # Failure in call to Scout API to stage files
# JOB_SCOUT_FILE_NOT_FOUND = 103  # One or more of the files requested for staging were not found on the Scout filesystem
JOB_CREATION_EXCEPTION = 199    # An exception occurred while creating the job. Comment field will contain exception traceback

DUMMY_TOKEN = "This is a real token, honest!"    # Used by the dummy Scout API endpoints


###########################################################################
# Class to handle Scout authorisation by adding the token to the headers
#

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


########################################################################
# Utility functions

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
            LOGGER.debug('Got new Scout token.')
            return SCOUT_API_TOKEN
        else:
            LOGGER.error('Failed to get Scout token: %d:%s' % (result.status_code, result.text))


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
        LOGGER.debug('Sending notification for job %d to %s(%s): %s' % (job_id, notify_url, config.RESULT_USERNAME, data))
        result = requests.post(notify_url, json=data, verify=False, auth=(config.RESULT_USERNAME, config.RESULT_PASSWORD))
    except requests.Timeout:
        LOGGER.error('Timout for job %d calling notify_url: %s' % (job_id, notify_url))
        return None
    except requests.RequestException:
        LOGGER.error('Exception for job %d calling notify_url %s: %s' % (job_id, notify_url, traceback.format_exc()))
        return

    if result.status_code != 200:
        LOGGER.error('Error %d returned for job %d when calling notify_url: %s' % (result.status_code, job_id, result.text))
    else:
        return True


def create_job(job: models.NewJob):
    """
    Carries out the actual job of finding out what files are to include in the job, asking Scout to stage those
    files, and creating the new job record in the database. Runs in the background after the new_job endpoint
    has returned.

    If job.files exists, those files are staged, and job.obs is ignored.

    If job.obs exists, then it is passed to the get_files() function (telescope specific), which is used to look up
    the list of files to stage.

    Any errors are passed back to the client by calling job.notify_url with an error code and comment - one of:
        JOB_FILE_LOOKUP_FAILED
        JOB_NO_FILES
        JOB_SCOUT_CALL_FAILED
        JOB_CREATION_EXCEPTION

    :param job: An instance of the NewJob() class.
    :return: None - runs in the background after the new_job endpoint has already returned.
    """
    pathlist = []
    if job.files:
        pathlist = job.files
    elif job.obs:
        pathlist = get_files(job.obs)

    if pathlist is None:
        send_result(notify_url=job.notify_url,
                    job_id=job.job_id,
                    return_code=JOB_FILE_LOOKUP_FAILED,
                    comment='Failed to get files associated with the given observation.')
    elif not pathlist:
        send_result(notify_url=job.notify_url,
                    job_id=job.job_id,
                    return_code=JOB_NO_FILES,
                    comment='No files to stage.')
    else:
        db = staged_db.DBPOOL.getconn()
        try:
            with db:
                with db.cursor() as curs:
                    curs.execute(staged_db.CREATE_JOB, (job.job_id,  # job_id
                                                        job.notify_url,  # URL to call on success/failure
                                                        datetime.datetime.now(timezone.utc),  # created
                                                        len(pathlist)))  # total_files
                    psycopg2.extras.execute_values(curs, staged_db.WRITE_FILES,
                                                   [(job.job_id, f, False, False) for f in pathlist])
            LOGGER.info('Job %d created.' % job.job_id)

            data = {'path': pathlist, 'copy': 0, 'inode': [], 'key':str(job.job_id), 'topic':config.KAFKA_TOPIC}
            result = requests.put(config.SCOUT_STAGE_URL, json=data, auth=ScoutAuth(get_scout_token()), verify=False)
            if result.status_code == 403:
                result = requests.put(config.SCOUT_STAGE_URL, json=data, auth=ScoutAuth(get_scout_token(refresh=True)), verify=False)
            LOGGER.debug('Got result for job %d from Scout API batchstage call: %d:%s' % (job.job_id, result.status_code, result.text))

            if result.status_code == 200:   # All files requested to be staged
                LOGGER.info('Job %d - initial Scout staging call succeeded' % job.job_id)
            else:
                LOGGER.info('Job %d - initial Scout staging call failed: %s' % (job.job_id, result.text))
        except:
            exc_str = traceback.format_exc()
            send_result(notify_url=job.notify_url,
                        job_id=job.job_id,
                        return_code=JOB_CREATION_EXCEPTION,
                        comment='Exception while trying to create job %d: %s' % (job.job_id, exc_str))
            LOGGER.error('Exception while trying to create job %d: %s' % (job.job_id, exc_str))

        finally:   # Return the DB connection
            staged_db.DBPOOL.putconn(db)


###############################################################################
# FastAPI endpoint definitions
#

app = FastAPI()


@app.on_event("startup")
async def startup():
    """Run on FastAPI startup, to create a Postgres connection pool.
    """
    staged_db.initdb(config)


@app.on_event("shutdown")
async def shutdown():
    """Run on FastAPI shutdown, to close all of the connections in the Postgres connection pool.
    """
    staged_db.DBPOOL.closeall()


@app.post("/staging/",
          status_code=status.HTTP_202_ACCEPTED,
          responses={403:{'model':models.ErrorResult}, 500:{'model':models.ErrorResult}, 502:{'model':models.ErrorResult}})
@app.post("/staging",
          include_in_schema=False,
          status_code=status.HTTP_202_ACCEPTED,
          responses={403:{'model': models.ErrorResult}, 500:{'model': models.ErrorResult}, 502:{'model': models.ErrorResult}})
async def new_job(job: models.NewJob, background_tasks: BackgroundTasks, response:Response):
    """
    POST API to create a new staging job. Accepts a JSON dictionary defined above by the Job() class,
    which is processed by FastAPI and passed to this function as an actual instance of the Job() class.

    Most of the work involved is done in the create_job() function, called in the background after this new_job()
    endpoint returns. Failures in that background task will be returned by calling the notify_url passed with the new
    job details.

    This endpoint will return:
        403 if the job ID already exists,
        400 if neither a file list or an observation structure is given,
        500 if there is an exception.

    Any errors in the background job creation process will be returned by calling job.notify_url
    \f
    :param job: An instance of the Job() class. Job.job_id is the new job ID, Job.files is a list of file names.
                job.obs is a telescope-specific structure defining an observation.
    :param background_tasks: An object to which this function can add tasks to be carried out in the background.
    :param response: An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    db = staged_db.DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute('SELECT count(*) FROM staging_jobs WHERE job_id = %s', (job.job_id,))
                if curs.fetchone()[0] > 0:
                    response.status_code = status.HTTP_403_FORBIDDEN
                    err_msg = "Can't create job %d, because it already exists." % job.job_id
                    return models.ErrorResult(errormsg=err_msg)

        if (not job.files) and (not job.obs):
            err_msg = "Error for job %d: Must specify either an observation, or a list of files" % job.job_id
            response.status_code = status.HTTP_400_BAD_REQUEST
            LOGGER.error(err_msg)
            return models.ErrorResult(errormsg=err_msg)

        background_tasks.add_task(create_job, job=job)
        return

    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error("Exception creating job %d: %s" % (job.job_id, exc_str))
        return models.ErrorResult(errormsg='Exception creating job %d: %s' % (job.job_id, exc_str))

    finally:   # Return the DB connection
        staged_db.DBPOOL.putconn(db)


@app.get("/staging/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.JobStatus}, 404:{'model':models.ErrorResult}, 500:{'model':models.ErrorResult}})
@app.get("/staging",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.JobStatus}, 404:{'model':models.ErrorResult}, 500:{'model':models.ErrorResult}})
def read_status(job_id: int, response:Response, include_files: bool = False):
    """
    GET API to read status details about an existing staging job. Accepts job_id and (optional) include_files
    as parameters in the URL, and looks up that job's data. The job status is returned as a
    JSON dict as defined by the JobStatus class. If you pass True in the 'include_files' parameter, then
    a complete list of all files in the job will be returned in the status structure.
    \f
    :param job_id:          Integer ASVO job ID
    :param include_files:   Pass True if the complete list of files should be included in the result
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return:                JSON dict as defined by the JobStatus() class above
    """
    db = staged_db.DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute(staged_db.QUERY_JOB, (job_id,))
                rows = curs.fetchall()
                if not rows:   # Job ID not found
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return models.ErrorResult(errormsg='Job %d not found' % job_id)
                if len(rows) > 1:   # Multiple rows for the same Job ID
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return models.ErrorResult(errormsg='Job %d has multiple rows!' % job_id)
                created, completed, total_files = rows[0]

                curs.execute('SELECT count(*) from files where job_id=%s and ready', (job_id,))
                ready_files = curs.fetchall()[0][0]

                curs.execute('SELECT count(*) from files where job_id=%s and error', (job_id,))
                error_files = curs.fetchall()[0][0]

                files = {}
                if include_files:
                    curs.execute(staged_db.QUERY_FILES, (job_id,))
                    for row in curs:
                        filename, ready, error, readytime = row
                        if readytime is not None:
                            readytime = int(readytime)
                        files[filename] = (ready, error, readytime)

                result = models.JobStatus(job_id=job_id,
                                          created=int(created),
                                          completed=completed,
                                          total_files=total_files,
                                          ready_files=ready_files,
                                          error_files=error_files)
                LOGGER.info("Job %d STATUS: %s" % (job_id, result))
                result.files = files
                return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting status for job %d: %s' % (job_id, exc_str))
        return models.ErrorResult(errormsg='Exception getting status for job %d: %s' % (job_id, exc_str))

    finally:   # Return the DB connection
        staged_db.DBPOOL.putconn(db)


@app.delete("/staging/",
            status_code=status.HTTP_200_OK,
            responses={404:{'model':models.ErrorResult}, 500:{'model':models.ErrorResult}})
@app.delete("/staging",
            include_in_schema=False,
            status_code=status.HTTP_200_OK,
            responses={404:{'model':models.ErrorResult}, 500:{'model':models.ErrorResult}})
def delete_job(job_id: int, response:Response):
    """
    POST API to delete a staging job. Accepts an integer job_id value, to be deleted.
    \f
    :param job_id:    # Integer ASVO job ID
    :param response:  # An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    db = staged_db.DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute(staged_db.QUERY_UNIQUE_FILES, (job_id, job_id))
                pathlist = []
                for row in curs:
                    filename = row[0]
                    pathlist.append(filename)

        LOGGER.info('Cancelling staging requests for %d files in job %d.' % (len(pathlist), job_id))
        data = {'path': pathlist, 'copy': 0, 'inode': [], 'key': str(job_id), 'topic': config.KAFKA_TOPIC}
        result = requests.put(config.SCOUT_CANCEL_URL,
                              json=data,
                              auth=ScoutAuth(get_scout_token()),
                              verify=False)
        if result.status_code == 403:
            result = requests.put(config.SCOUT_CANCEL_URL,
                                  json=data,
                                  auth=ScoutAuth(get_scout_token(refresh=True)),
                                  verify=False)
        LOGGER.debug('Got result for job %d from Scout API cancelbatchstage call: %d:%s' % (job_id, result.status_code, result.text))

        with db:
            with db.cursor() as curs:
                curs.execute(staged_db.DELETE_JOB, (job_id,))
                if curs.rowcount == 0:
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return models.ErrorResult(errormsg='Job %d not found' % job_id)
                curs.execute(staged_db.DELETE_FILES, (job_id,))
                LOGGER.info('Job %d DELETED.' % job_id)
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception deleting job %d: %s' % (job_id, exc_str))
        return models.ErrorResult(errormsg='Exception deleting job %d: %s' % (job_id, exc_str))

    finally:   # Return the DB connection
        staged_db.DBPOOL.putconn(db)


@app.get("/ping/")
@app.get("/ping", include_in_schema=False)
def ping():
    """
    GET API to return nothing, with a 200 status code, to show that we are alive.
    \f
    :return: None
    """
    LOGGER.info('Ping called.')
    return


def sanitise(value):
    """
    Take a value returned from Scout, and if it's not None, convert from string to integer.

    :param value: String representation of an integer, or None
    :return: integer, or None
    """
    if value is not None:
        try:
            return int(value)
        except ValueError:
            return value
    else:
        return None


@app.get("/get_stats/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.GlobalStatistics}, 500:{'model':models.ErrorResult}})
@app.get("/get_stats",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.GlobalStatistics}, 500:{'model':models.ErrorResult}})
def get_stats(response:Response):
    """
    GET API to return an overall job and file statistics for this process (the staging web server)
    and the Kafka daemon (kafkad.py)
    \f
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: GlobalStatus object
    """
    db = staged_db.DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute("SELECT update_time, last_message, kafka_alive FROM kafkad_heartbeat")
                kafkad_heartbeat, last_message, kafka_alive = curs.fetchall()[0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE completed")
                completed_jobs = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE not completed")
                incomplete_jobs = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM files WHERE ready")
                ready_files = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM files WHERE error")
                error_files = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM files WHERE not ready and not error")
                waiting_files = curs.fetchall()[0][0]

        if kafkad_heartbeat is not None:
            hb = kafkad_heartbeat.timestamp()
        else:
            hb = None
        if last_message is not None:
            lm = last_message.timestamp()
        else:
            lm = None

        result = models.GlobalStatistics(kafkad_heartbeat=hb,
                                         kafka_alive=kafka_alive,
                                         last_message=lm,
                                         completed_jobs=completed_jobs,
                                         incomplete_jobs=incomplete_jobs,
                                         ready_files=ready_files,
                                         error_files=error_files,
                                         waiting_files=waiting_files)

        LOGGER.info('Getting cache status ')
        cache_result = requests.get(config.SCOUT_CACHESTATE_URL,
                                    auth=ScoutAuth(get_scout_token()),
                                    verify=False)
        if cache_result.status_code == 401:
            cache_result = requests.get(config.SCOUT_CACHESTATE_URL,
                                        auth=ScoutAuth(get_scout_token(refresh=False)),
                                        verify=False)
        if cache_result.status_code == 200:
            resdict = cache_result.json()
            result.availdatasz = sanitise(resdict.get('availdatasz', None))    # scoutfs available data capacity
            result.health = sanitise(resdict.get('health', None))              # scoutfs health of filesystem: 0-unknown, 1-ok, 2-warning, 3-error
            result.healthstatus = resdict.get('healthstatus', None)            # scoutfs health status info string
            result.releaseable = sanitise(resdict.get('releaseable', None))    # scoutfs capacity of online files eligible for release (archive complete)
            result.nonreleaseable = sanitise(resdict.get('nonreleaseable', None))    # scoutfs capacity of online files eligible for release (archive complete)
            result.pending = sanitise(resdict.get('pending', None))    # scoutfs capacity of online files eligible for release (archive complete)
            result.totdatasz = sanitise(resdict.get('totdatasz', None))        # scoutfs total data capacity
        else:
            LOGGER.error('Got status code %d from Scout cache status call: %s' % (cache_result.status_code, cache_result.text))

        LOGGER.debug('Called get_stats: %s' % result)
        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting staging system status: %s' % (exc_str,))
        return models.ErrorResult(errormsg='Exception getting staging system status: %s' % (exc_str,))

    finally:   # Return the DB connection
        staged_db.DBPOOL.putconn(db)


@app.get("/get_joblist/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.JobList}, 500:{'model':models.ErrorResult}})
@app.get("/get_joblist",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':models.JobList}, 500:{'model':models.ErrorResult}})
def get_joblist(response:Response):
    """
    GET API to return a list of all jobs, giving the number of staged files, the total number of files, and the
    completed status flag for each job.
    \f
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: JobList object
    """
    db = staged_db.DBPOOL.getconn()
    try:
        result = models.JobList()
        with db:
            with db.cursor() as curs:
                curs.execute(staged_db.LIST_JOBS)
                for row in curs.fetchall():
                    job_id, staged_files, total_files, completed = row
                    result.jobs[job_id] = (staged_files, total_files, completed)

        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting staging job list: %s' % (exc_str,))
        return models.ErrorResult(errormsg='Exception getting staging job list: %s' % (exc_str,))

    finally:   # Return the DB connection
        staged_db.DBPOOL.putconn(db)


@app.post("/filestatus/",
          status_code=status.HTTP_200_OK,
          responses={200: {'model': models.FileStatusResult}, 404: {'model': models.ErrorResult},
                     500: {'model': models.ErrorResult}})
@app.post("/filestatus",
          include_in_schema=False,
          status_code=status.HTTP_200_OK,
          responses={200: {'model': models.FileStatusResult}, 404: {'model': models.ErrorResult},
                     500: {'model': models.ErrorResult}})
async def file_status(data: models.FileStatus, response: Response):
    """
    GET API to return the status of one or more files, using the Scout API to query the
    file status.

    This endpoint will return:
        200 with the status structure if all the files exist,
        404 if one or more files was not found,
        500 if there is an exception.

    \f
    :param data: A structure containing one member, 'files', containing a list of filenames
    :param response: An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    try:
        allresults = {}
        for filename in data.files:
            LOGGER.info('Getting status for %s' % str(filename))
            result = requests.get(config.SCOUT_QUERY_URL, params={'path': filename}, auth=ScoutAuth(get_scout_token()),
                                  verify=False)
            if result.status_code == 401:
                result = requests.get(config.SCOUT_QUERY_URL, params={'path': filename},
                                      auth=ScoutAuth(get_scout_token(refresh=False)), verify=False)
            if result.status_code != 200:
                response.status_code = status.HTTP_404_NOT_FOUND
                LOGGER.error('Scout call failed during a file status request: %d:%s' % (result.status_code, result.text))
                return models.ErrorResult(errormsg="Scout call failed during a file status request: %d:%s" % (result.status_code, result.text))

            resdict = result.json()
            offlineblocks = resdict.get('offlineblocks', None)
            allresults[filename] = (offlineblocks == 0)
        return allresults
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error("Exception getting file status: %s" % exc_str)
        return models.ErrorResult(errormsg="Exception getting file status: %s" % exc_str)


########################################################################################################
#
#    Dummy endpoints, emulating the Scout API for testing.
#
########################################################################################################

@app.post("/v1/security/login/",
          status_code=status.HTTP_200_OK,
          response_model=models.ScoutLoginResponse)
@app.post("/v1/security/login",
          include_in_schema=False,
          status_code=status.HTTP_200_OK,
          response_model=models.ScoutLoginResponse)
def dummy_scout_login(account: models.ScoutLogin, response:Response):
    """
    Emulate Scout API for development. Takes an account name and password, returns a token string.
    \f
    :param account: ScoutLogin (keys are 'acct' and 'pass')
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: Token string in a dict, with key 'response'
    """
    if (account.acct == config.SCOUT_API_USER) and (account.passwd == config.SCOUT_API_PASSWORD):
        LOGGER.info('Pretending be Scout: /v1/security/login passed, returning token')
        return models.ScoutLoginResponse(response=DUMMY_TOKEN)
    else:
        LOGGER.error('Pretending be Scout: /v1/security/login failed, Bad username/password')
        response.status_code = 401
        return


@app.post("/v1/request/batchstage/",
          status_code=status.HTTP_200_OK)
@app.post("/v1/request/batchstage",
          include_in_schema=False,
          status_code=status.HTTP_200_OK)
def dummy_scout_stage(stagedata: models.StageFiles, request:Request, response:Response):
    """
    Emulate Scout's staging server for development. 'Stages' the files passed in the list of filenames.

    Always returns 200/OK and ignores the file list.
    \f
    :param stagedata: An instance of StageFiles containing the file list to stage
    :param request: FastAPI request object, so we can check for the right token
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    if request.headers['Authorization'] == 'Bearer %s' % DUMMY_TOKEN:
        LOGGER.info("Pretending be Scout: /v1/request/batchstage succeeded: %s" % (stagedata.path,))
    else:
        LOGGER.error("Pretending to be Scout: /v1/request/batchstage failed: bad Authorization header: %s" % request.headers['Authorization'])
        response.status_code = 403


@app.get("/v1/file/",
         status_code=status.HTTP_200_OK,
         response_model=models.ScoutFileStatus)
@app.get("/v1/file",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         response_model=models.ScoutFileStatus)
def dummy_scout_status(request:Request, response:Response, path: str = Query(...)):
    """
    Emulate Scout's staging server for development. Returns a status structure for the given (single) file.

    Returns a dummy status for the file specified - if the filename starts with 'offline_', then the status
    for the file has 1 block still offline, otherwise the status shows the file is online.
    \f
    :param path: Filename to check the status for. If it starts with 'offline_', then the returned status will be offline
    :param request: FastAPI request object, so we can check for the right token
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    if request.headers['Authorization'] == 'Bearer %s' % DUMMY_TOKEN:
        resp = models.ScoutFileStatus(path=path)
        resp.onlineblocks = 22
        if path.startswith('offline_'):
            resp.offlineblocks = 1
        else:
            resp.offlineblocks = 0
        LOGGER.info("Pretending be Scout: /v1/file succeeded for %s" % (path,))
        return resp
    else:
        LOGGER.error("Pretending to be Scout: /v1/file succeeded: bad Authorization header: %s" % request.headers['Authorization'])
        response.status_code = 403


########################################################################################################
#
#    Dummy endpoint, emulating ASVO to use as a notify_url endpoint for testing
#
########################################################################################################

@app.post("/jobresult/",
          status_code=status.HTTP_200_OK)
@app.post("/jobresult",
          include_in_schema=False,
          status_code=status.HTTP_200_OK)
def dummy_asvo(result: models.JobResult):
    """
    Emulate ASVO server for development. Used to pass a job result back to ASVO, when all files are staged, or when
    the job times out.

    Always returns 200/OK.
    \f
    :param result: An instance of JobResult
    :return: None
    """
    LOGGER.info("Pretending to be ASVO: Job result received: %s" % (result,))
