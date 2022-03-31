"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

import datetime
from datetime import timezone
import os
from typing import Optional
import logging
from logging import handlers
import traceback
from threading import Semaphore

from fastapi import FastAPI, BackgroundTasks, Query, Request, Response, status
import psycopg2
from psycopg2 import pool
from psycopg2 import extras
from pydantic import BaseModel
import requests
from requests.auth import AuthBase
from urllib3.exceptions import InsecureRequestWarning

from mwa_files import MWAObservation as Observation
from mwa_files import get_mwa_files as get_files

MINCONN = 2          # Start with this many database connections
MAXCONN = 100         # Don't ever create more than this many

DBPOOL = pool.ThreadedConnectionPool(0, 0)    # Will be a real, connected psycopg2 database connection pool object after startup

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
        self.SCOUT_LOGIN_URL = os.getenv('SCOUT_LOGIN_URL')
        self.SCOUT_API_USER = os.getenv('SCOUT_API_USER')
        self.SCOUT_API_PASSWORD = os.getenv('SCOUT_API_PASSWORD')

        self.KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

        self.RESULT_USERNAME = os.getenv('RESULT_USERNAME')
        self.RESULT_PASSWORD = os.getenv('RESULT_PASSWORD')


config = ApiConfig()

SCOUT_API_TOKEN = ''

CREATE_JOB = """
INSERT INTO staging_jobs (job_id, notify_url, created, completed, total_files, notified, checked)
VALUES (%s, %s, %s, false, %s, false, false)
"""

DELETE_JOB = """
DELETE FROM staging_jobs
WHERE job_id = %s
"""

WRITE_FILES = """
INSERT INTO files (job_id, filename, ready, error) 
VALUES %s
"""

DELETE_FILES = """
DELETE FROM files
WHERE job_id = %s
"""

QUERY_JOB = """
SELECT extract(epoch from created), completed, notified, total_files
FROM staging_jobs
WHERE job_id = %s
"""

LIST_JOBS = """
SELECT sj.job_id, 
       (SELECT count(*) FROM files WHERE files.job_id=sj.job_id AND ready) as staged_files, 
       sj.total_files, 
       sj.completed 
FROM staging_jobs as sj
ORDER BY sj.job_id;
"""

QUERY_FILES = """
SELECT filename, ready, error, extract(epoch from readytime)
FROM files
WHERE job_id = %s
"""

# Job return codes:

JOB_SUCCESS = 0                 # All files staged successfully
JOB_TIMEOUT = 10                # Timeout waiting for all files to stage - comment field will contain number of staged and outstanding files
JOB_FILE_LOOKUP_FAILED = 100    # Failed to look up files associated with the given observation (eg failure in metadata/data_files web service call)
JOB_NO_FILES = 101              # No files to stage
JOB_SCOUT_CALL_FAILED = 102     # Failure in call to Scout API to stage files
JOB_SCOUT_FILE_NOT_FOUND = 103  # One or more of the files requested for staging were not found on the Scout filesystem
JOB_CREATION_EXCEPTION = 199    # An exception occurred while creating the job. Comment field will contain exception traceback

DUMMY_TOKEN = "This is a real token, honest!"    # Used by the dummy Scout API endpoints


#####################################################################
# Pydantic models defining FastAPI parameters or responses
#


class NewJob(BaseModel):
    """
    Used by the 'new job' endpoint to pass in the job ID and either a list of files, or a telescope specific structure
    defining an observation, where all files that are part of that observation should be staged.

    job_id:      Integer staging job ID\n
    files:       A list of filenames, including full paths\n
    obs:         Telescope-specific structure defining an observation whose files you want staged\n
    """
    job_id: int                       # Integer staging job ID
    files: Optional[list[str]] = []   # A list of filenames, including full paths
    notify_url: str                   # URL to call to notify the client about job failure or success
    obs: Optional[Observation] = {}   # A telescope-specific structure defining an observation whose files you want staged


class JobResult(BaseModel):
    """
    Used by the dummy ASVO server endpoint to pass in the results of a job once all files are staged.

    job_id Integer job ID\n
    return_code: Integer error code, eg JOB_SUCCESS=0\n
    total_files: Total number of files in job\n
    ready_files: Number of files successfully staged\n
    error_files: Number of files where Kafka returned an error\n
    comment: Human readable string (eg error message)\n
    """
    job_id: int      # Integer staging job ID
    return_code: int  # Integer error code, eg JOB_SUCCESS=0
    total_files: int   # Total number of files in job
    ready_files: int   # Number of files successfully staged
    error_files: int   # Number of files where Kafka returned an error
    comment: str     # Human readable string (eg error message)


class StageFiles(BaseModel):
    """
    Used by the dummy ASVO server endpoint to pretend to stage jobs.
    """
    path: Optional[list[str]] = Query(None)
    copyint: int = Query(0, alias='copy')
    inode: Optional[list[int]] = Query(None)


class JobStatus(BaseModel):
    """
    Used by the 'read status' endpoint to return the status of a single job.

    job_id:       Integer ASVO job ID\n
    created:      Integer unix timestamp when the job was created\n
    completed:    True if all files have been staged\n
    total_files:  Total number of files in this job\n
    ready_files: Number of files successfully staged\n
    error_files: Number of files where Kafka returned an error\n
    files:        key is filename, value is tuple(bool, bool, int), which contains (ready, error, readytime)\n

    The files attribute is a list of (ready:bool, readytime:int) tuples where ready_time is the time when that file
    was staged (or None)
    """
    job_id: int       # Integer staging job ID
    created: Optional[int] = None      # Integer unix timestamp when the job was created
    completed: Optional[bool] = None   # True if all files have been staged
    total_files: Optional[int] = None  # Total number of files in this job
    ready_files: Optional[int] = None   # Number of files successfully staged
    error_files: Optional[int] = None   # Number of files where Kafka returned an error
    # The files attribute is a list of (ready:bool, error:bool, readytime:int) tuples where ready_time
    # is the time when that file was staged or returned an error (or None if neither has happened yet)
    files: dict[str, tuple] = {"":(False, False, 0)}     # Specifying the tuple type as tuple(bool, int) crashes the FastAPI doc generator


class GlobalStatistics(BaseModel):
    """
    Used by the 'get stats' endpoint to return the kafka/kafkad/staged status and health data.

    kafkad_heartbeat:   Timestamp when kafkad.py last updated its status table\n
    kafka_alive: bool   Is the remote kafka daemon alive?\n
    last_message:       Timestamp when the last valid file status message was received\n
    completed_jobs:     Total number of jobs with all files staged\n
    incomplete_jobs:    Number of jobs waiting for files to be staged\n
    ready_files: i      Total number of files staged so far\n
    unready_files:      Number of files we are waiting to be staged\n
    """
    kafkad_heartbeat: Optional[float]  # Timestamp when kafkad.py last updated its status table
    kafka_alive: bool = False          # Is the remote kafka daemon alive?
    last_message: Optional[float]      # Timestamp when the last valid file status message was received
    completed_jobs: int = 0            # Total number of jobs with all files staged
    incomplete_jobs: int = 0           # Number of jobs waiting for files to be staged
    ready_files: int = 0               # Total number of files staged successfully so far
    error_files: int = 0               # Number of files for which Kafka reported an error
    waiting_files: int = 0             # Number of files we have still waiting to be staged


class JobList(BaseModel):
    """
    Used by the 'list all jobs' endoint to return a list of all jobs.

    jobs:   Key is job ID, value is (staged_files, total_files, completed)
    """
    jobs: dict[int, tuple] = {}  # Key is job ID, value is (staged_files, total_files, completed)


class ErrorResult(BaseModel):
    """
    Used by all jobs, to return an error message if the call failed.

    errormsg:  Error message returned to caller
    """
    errormsg: str   # Error message returned to caller


class ScoutFileStatus(BaseModel):
    """
    Used by the Scout API dummy status endpoint, to return the status of a single file.

    archdone:        All copies complete, file exists on tape (possibly on disk as well)\n
    path:            First file name for file, for policy decisions, eg "/object.dat.0"\n
    size:            File size in bytes, eg "10485760"\n
    inode:           File inode number, eg "2"\n
    uid:             User ID for file, eg "0"\n
    username:        User name for file, eg "root"\n
    gid:             group ID for file, eg "0"\n
    groupname:       group name for file, eg "root"\n
    mode:            file mode, eg 33188\n
    modestring:      file mode string, eg "-rw-r--r--"\n
    nlink:           number of links to file, eg 1\n
    atime:           access time, eg "1618411999"\n
    mtime:           modify time, eg "1618411968"\n
    ctime:           attribute change time, eg "1618415117"\n
    rtime:           residence time, eg "1618415102"\n
    version:         Data version, eg "2560"\n
    onlineblocks:    Number of online blocks, eg "2560"\n
    offlineblocks:   Number of offline blocks, eg "0"\n
    flags:           archive flags, eg "0"\n
    flagsstring:     archive flags string, eg ""\n
    uuid:            UUID for file, eg "8e9825f9-291d-415e-bb1f-38d8f865dcac"\n
    copies:          Complex sub-structure that we'll ignore for this dummy service
    """
    archdone: bool = True     # All copies complete, file exists on tape (possibly on disk as well)
    path: str                 # First file name for file, for policy decisions, eg "/object.dat.0"
    size: int = 0             # File size in bytes, eg "10485760"
    inode: int = 0            # File inode number, eg "2"
    uid: int = 0              # User ID for file, eg "0"
    username: str = ""        # User name for file, eg "root"
    gid: int = 0              # group ID for file, eg "0"
    groupname: str = ""       # group name for file, eg "root"
    mode: int = 0             # file mode, eg 33188
    modestring: str = ""      # file mode string, eg "-rw-r--r--"
    nlink: int = 0            # number of links to file, eg 1
    atime: int = 0            # access time, eg "1618411999"
    mtime: int = 0            # modify time, eg "1618411968"
    ctime: int = 0            # attribute change time, eg "1618415117"
    rtime: int = 0            # residence time, eg "1618415102"
    version: str = ""         # Data version, eg "2560"
    onlineblocks: int = 0     # Number of online blocks, eg "2560"
    offlineblocks: int = 0    # Number of offline blocks, eg "0"
    flags: int = 0            # archive flags, eg "0"
    flagsstring: str = ""     # archive flags string, eg ""
    uuid: str = ""            # UUID for file, eg "8e9825f9-291d-415e-bb1f-38d8f865dcac"
    copies: list = []         # Complex sub-structure that we'll ignore for this dummy service


class ScoutLogin(BaseModel):
    """
    Used by the dummy Scout API login endpoint, to pass username/password
    """
    acct: str      # Account name
    passwd: str = Query('', alias='pass')     # Account password


class ScoutLoginResponse(BaseModel):
    """
    Used by the dummy Scout API login endoint to return the token
    """
    response: str    # Token string


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

class ReallyThreadedConnectionPool(pool.ThreadedConnectionPool):
    """
    Block if we exceed the maximum number of connections, instead of throwing an exception.

    From: https://stackoverflow.com/questions/48532301/python-postgres-psycopg2-threadedconnectionpool-exhausted
    """
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self._semaphore = Semaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        return super().getconn(*args, **kwargs)

    def putconn(self, *args, **kwargs):
        super().putconn(*args, **kwargs)
        self._semaphore.release()


def initdb(minconn=MINCONN, maxconn=MAXCONN):
    """
    Initialise a connection pool to the database.

    :param minconn: Minimum number of connections. Open this many at startup, and close connections down to this many
                    as they are returned.
    :param maxconn: Maximum number of connections to open - getconn() will fail if this many are already open.
    :return: None
    """
    dbpool = ReallyThreadedConnectionPool(minconn=minconn,
                                          maxconn=maxconn,
                                          host=config.DBHOST,
                                          user=config.DBUSER,
                                          database=config.DBNAME,
                                          password=config.DBPASSWORD)
    return dbpool


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


def create_job(job: NewJob):
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
        db = DBPOOL.getconn()
        try:
            with db:
                data = {'path': pathlist, 'copy': 0, 'inode': [], 'key':str(job.job_id), 'topic':config.KAFKA_TOPIC}
                result = requests.put(config.SCOUT_STAGE_URL, json=data, auth=ScoutAuth(get_scout_token()), verify=False)
                if result.status_code == 403:
                    result = requests.put(config.SCOUT_STAGE_URL, json=data, auth=ScoutAuth(get_scout_token(refresh=True)), verify=False)
                LOGGER.debug('Got result for job %d from Scout API batchstage call: %d:%s' % (job.job_id, result.status_code, result.text))

                if result.status_code == 200:   # All files requested to be staged
                    with db.cursor() as curs:
                        curs.execute(CREATE_JOB, (job.job_id,  # job_id
                                                  job.notify_url,  # URL to call on success/failure
                                                  datetime.datetime.now(timezone.utc),  # created
                                                  len(pathlist)))  # total_files
                        psycopg2.extras.execute_values(curs, WRITE_FILES, [(job.job_id, f, False, False) for f in pathlist])
                        LOGGER.info('Job %d created.' % job.job_id)
                else:
                    send_result(notify_url=job.notify_url,
                                job_id=job.job_id,
                                return_code=JOB_SCOUT_CALL_FAILED,
                                comment='Scout API returned an error: %s' % result.text)
                    LOGGER.info('Job %d NOT created, Scout call failed: %s' % (job.job_id, result.text))
        except:
            exc_str = traceback.format_exc()
            send_result(notify_url=job.notify_url,
                        job_id=job.job_id,
                        return_code=JOB_CREATION_EXCEPTION,
                        comment='Exception while trying to create job %d: %s' % (job.job_id, exc_str))
            LOGGER.error('Exception while trying to create job %d: %s' % (job.job_id, exc_str))

        finally:   # Return the DB connection
            DBPOOL.putconn(db)


###############################################################################
# FastAPI endpoint definitions
#

app = FastAPI()


@app.on_event("startup")
async def startup():
    """Run on FastAPI startup, to create a Postgres connection pool.
    """
    global DBPOOL
    DBPOOL = initdb(MINCONN, MAXCONN)


@app.on_event("shutdown")
async def startup():
    """Run on FastAPI startup, to create a Postgres connection pool.
    """
    global DBPOOL
    DBPOOL.closeall()


@app.post("/staging/",
          status_code=status.HTTP_202_ACCEPTED,
          responses={403:{'model':ErrorResult}, 500:{'model':ErrorResult}, 502:{'model':ErrorResult}})
@app.post("/staging",
          include_in_schema=False,
          status_code=status.HTTP_202_ACCEPTED,
          responses={403:{'model': ErrorResult}, 500:{'model': ErrorResult}, 502:{'model': ErrorResult}})
async def new_job(job: NewJob, background_tasks: BackgroundTasks, response:Response):
    """
    POST API to create a new staging job. Accepts a JSON dictionary defined above by the Job() class,
    which is processed by FastAPI and passed to this function as an actual instance of the Job() class.

    Most of the work involved is done in the create_job() function, called in the background after this new_job()
    endpoint returns. Failures in that background task will be returned by calling the notify_url passed with the new
    job details.

    This endpoint will return:
        403 if the job ID already exists
        400 if neither a file list or an observation structure is given
        500 if there is an exception

    Any errors in the background job creation process will be returned by calling job.notify_url
    \f
    :param job: An instance of the Job() class. Job.job_id is the new job ID, Job.files is a list of file names.
                job.obs is a telescope-specific structure defining an observation.
    :param background_tasks: An object to which this function can add tasks to be carried out in the background.
    :param response: An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    db = DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute('SELECT count(*) FROM staging_jobs WHERE job_id = %s' % (job.job_id,))
                if curs.fetchone()[0] > 0:
                    response.status_code = status.HTTP_403_FORBIDDEN
                    err_msg = "Can't create job %d, because it already exists." % job.job_id
                    return ErrorResult(errormsg=err_msg)

        if (not job.files) and (not job.obs):
            err_msg = "Error for job %d: Must specify either an observation, or a list of files" % job.job_id
            response.status_code = status.HTTP_400_BAD_REQUEST
            LOGGER.error(err_msg)
            return ErrorResult(errormsg=err_msg)

        background_tasks.add_task(create_job, job=job)
        return

    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error("Exception creating job %d: %s" % (job.job_id, exc_str))
        return ErrorResult(errormsg='Exception creating job %d: %s' % (job.job_id, exc_str))

    finally:   # Return the DB connection
        DBPOOL.putconn(db)


@app.get("/staging/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobStatus}, 404:{'model':ErrorResult}, 500:{'model':ErrorResult}})
@app.get("/staging",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobStatus}, 404:{'model':ErrorResult}, 500:{'model':ErrorResult}})
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
    db = DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute(QUERY_JOB, (job_id,))
                rows = curs.fetchall()
                if not rows:   # Job ID not found
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return ErrorResult(errormsg='Job %d not found' % job_id)
                if len(rows) > 1:   # Multiple rows for the same Job ID
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return ErrorResult(errormsg='Job %d has multiple rows!' % job_id)
                created, completed, notified, total_files = rows[0]

                curs.execute('SELECT count(*) from files where job_id=%s and ready', (job_id,))
                ready_files = curs.fetchall()[0][0]

                curs.execute('SELECT count(*) from files where job_id=%s and error', (job_id,))
                error_files = curs.fetchall()[0][0]

                files = {}
                if include_files:
                    curs.execute(QUERY_FILES, (job_id,))
                    for row in curs:
                        filename, ready, error, readytime = row
                        if readytime is not None:
                            readytime = int(readytime)
                        files[filename] = (ready, error, readytime)

                result = JobStatus(job_id=job_id,
                                   created=int(created),
                                   completed=completed,
                                   notified=notified,
                                   total_files=total_files,
                                   ready_files=ready_files,
                                   error_files=error_files)
                LOGGER.info("Job %d STATUS: %s" % (job_id, result))
                result.files = files
                LOGGER.debug("Job %d file status: %s" % (job_id, files))
                return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting status for job %d: %s' % (job_id, exc_str))
        return ErrorResult(errormsg='Exception getting status for job %d: %s' % (job_id, exc_str))

    finally:   # Return the DB connection
        DBPOOL.putconn(db)


@app.delete("/staging/",
            status_code=status.HTTP_200_OK,
            responses={404:{'model':ErrorResult}, 500:{'model':ErrorResult}})
@app.delete("/staging",
            include_in_schema=False,
            status_code=status.HTTP_200_OK,
            responses={404:{'model':ErrorResult}, 500:{'model':ErrorResult}})
def delete_job(job_id: int, response:Response):
    """
    POST API to delete a staging job. Accepts an integer job_id value, to be deleted.
    \f
    :param job_id:    # Integer ASVO job ID
    :param response:  # An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    db = DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute(DELETE_JOB % (job_id,))
                if curs.rowcount == 0:
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return ErrorResult(errormsg='Job %d not found' % job_id)
                curs.execute(DELETE_FILES, (job_id,))
                LOGGER.info('Job %d DELETED.' % job_id)
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception deleting job %d: %s' % (job_id, exc_str))
        return ErrorResult(errormsg='Exception deleting job %d: %s' % (job_id, exc_str))

    finally:   # Return the DB connection
        DBPOOL.putconn(db)


@app.get("/ping/")
@app.get("/ping", include_in_schema=False)
def ping():
    """
    GET API to return nothing, with a 200 status code, if we are alive. If we're not alive, this function won't be
    called at all.
    \f
    :return: None
    """
    LOGGER.info('Ping called.')
    return


@app.get("/get_stats/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':GlobalStatistics}, 500:{'model':ErrorResult}})
@app.get("/get_stats",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':GlobalStatistics}, 500:{'model':ErrorResult}})
def get_stats(response:Response):
    """
    GET API to return an overall job and file statistics for this process (the staging web server)
    and the Kafka daemon (kafkad.py)
    \f
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: GlobalStatus object
    """
    db = DBPOOL.getconn()
    try:
        with db:
            with db.cursor() as curs:
                curs.execute("SELECT update_time, last_message, kafka_alive FROM kafkad_heartbeat")
                kafkad_heartbeat, last_message, kafka_alive = curs.fetchall()[0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE completed")
                completed_jobs = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE completed")
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

        result = GlobalStatistics(kafkad_heartbeat=hb,
                                  kafka_alive=kafka_alive,
                                  last_message=lm,
                                  completed_jobs=completed_jobs,
                                  incomplete_jobs=incomplete_jobs,
                                  ready_files=ready_files,
                                  error_files=error_files,
                                  waiting_files=waiting_files)
        LOGGER.info('Called get_stats.')
        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting staging system status: %s' % (exc_str,))
        return ErrorResult(errormsg='Exception getting staging system status: %s' % (exc_str,))

    finally:   # Return the DB connection
        DBPOOL.putconn(db)


@app.get("/get_joblist/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobList}, 500:{'model':ErrorResult}})
@app.get("/get_joblist",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobList}, 500:{'model':ErrorResult}})
def get_joblist(response:Response):
    """
    GET API to return a list of all jobs, giving the number of staged files, the total number of files, and the
    completed status flag for each job.
    \f
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: JobList object
    """
    db = DBPOOL.getconn()
    try:
        result = JobList()
        with db:
            with db.cursor() as curs:
                curs.execute(LIST_JOBS)
                for row in curs.fetchall():
                    job_id, staged_files, total_files, completed = row
                    result.jobs[job_id] = (staged_files, total_files, completed)

        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        LOGGER.error('Exception getting staging job list: %s' % (exc_str,))
        return ErrorResult(errormsg='Exception getting staging job list: %s' % (exc_str,))

    finally:   # Return the DB connection
        DBPOOL.putconn(db)


########################################################################################################
#
#    Dummy endpoints, emulating the Scout API for testing.
#
########################################################################################################

@app.post("/v1/security/login/",
          status_code=status.HTTP_200_OK,
          response_model=ScoutLoginResponse)
@app.post("/v1/security/login",
          include_in_schema=False,
          status_code=status.HTTP_200_OK,
          response_model=ScoutLoginResponse)
def dummy_scout_login(account: ScoutLogin, response:Response):
    """
    Emulate Scout API for development. Takes an account name and password, returns a token string.
    :param account: ScoutLogin (keys are 'acct' and 'pass')
    :param response:        An instance of fastapi.Response(), used to set the status code returned
    :return: Token string in a dict, with key 'response'
    """
    if (account.acct == config.SCOUT_API_USER) and (account.passwd == config.SCOUT_API_PASSWORD):
        LOGGER.info('Pretending be Scout: /v1/security/login passed, returning token')
        return ScoutLoginResponse(response=DUMMY_TOKEN)
    else:
        LOGGER.error('Pretending be Scout: /v1/security/login failed, Bad username/password')
        response.status_code = 401
        return


@app.post("/v1/request/batchstage/",
          status_code=status.HTTP_200_OK)
@app.post("/v1/request/batchstage",
          include_in_schema=False,
          status_code=status.HTTP_200_OK)
def dummy_scout_stage(stagedata: StageFiles, request:Request, response:Response):
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
         response_model=ScoutFileStatus)
@app.get("/v1/file",
         include_in_schema=False,
         status_code=status.HTTP_200_OK,
         response_model=ScoutFileStatus)
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
        resp = ScoutFileStatus(path=path)
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
def dummy_asvo(result: JobResult):
    """
    Emulate ASVO server for development. Used to pass a job result back to ASVO, when all files are staged, or when
    the job times out.

    Always returns 200/OK.
    \f
    :param result: An instance of JobResult
    :return: None
    """
    LOGGER.info("Pretending to be ASVO: Job result received: %s" % (result,))
