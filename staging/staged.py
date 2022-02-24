"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

import datetime
from datetime import timezone
import os
from typing import Optional

import traceback
from fastapi import FastAPI, BackgroundTasks, Query, Response, status
import psycopg2
from psycopg2 import extras
from pydantic import BaseModel
import requests
from requests.auth import AuthBase

from mwa_files import MWAObservation as Observation
from mwa_files import get_mwa_files as get_files


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

        self.RESULT_USERNAME = os.getenv('RESULT_USERNAME')
        self.RESULT_PASSWORD = os.getenv('RESULT_PASSWORD')


config = ApiConfig()

SCOUT_API_TOKEN = ''
DB = psycopg2.connect(user=config.DBUSER,
                      password=config.DBPASSWORD,
                      host=config.DBHOST,
                      database=config.DBNAME)

CREATE_JOB = """
INSERT INTO staging_jobs (job_id, notify_url, created, completed, total_files, notified)
VALUES (%s, %s, %s, false, %s, false)
"""

DELETE_JOB = """
DELETE FROM staging_jobs
WHERE job_id = %s
"""

WRITE_FILES = """
INSERT INTO files (job_id, filename, ready)
VALUES %s
"""

DELETE_FILES = """
DELETE FROM files
WHERE job_id = %s
"""

QUERY_JOB = """
SELECT extract(epoch from created), completed, total_files
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
SELECT filename, ready, extract(epoch from readytime)
FROM files
WHERE job_id = %s
"""

# Job error codes:

JOB_SUCCESS = 0                # All files staged successfully
JOB_TIMEOUT = 10               # Timeout waiting for all files to stage - comment field will contain number of staged and outstanding files
JOB_FILE_LOOKUP_FAILED = 100   # Failed to look up files associated with the given observation (eg failure in metadata/data_files web service call)
JOB_NO_FILES = 101             # No files to stage
JOB_SCOUT_CALL_FAILED = 102    # Failure in call to Scout API to stage files
JOB_CREATION_EXCEPTION = 199   # An exception occurred while creating the job. Comment field will contain exception traceback


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
    job_id: int                     # Integer staging job ID
    files: Optional[list[str]]      # A list of filenames, including full paths
    notify_url: str                 # URL to call to notify the client about job failure or success
    obs: Optional[Observation]      # A telescope-specific structure defining an observation whose files you want staged


class JobResult(BaseModel):
    """
    Used by the dummy ASVO server endpoint to pass in the results of a job once all files are staged.

    job_id Integer job ID\n
    ok: Boolean - True means the job has succeeded in staging all files, False means there was an error\n
    error_code: Integer error code, eg JOB_SUCCESS=0\n
    comment: Human readable string (eg error message)\n
    """
    job_id: int      # Integer staging job ID
    ok: bool         # True if the job succeeded in staging all files, False means there was an error.
    error_code: int  # Integer error code, eg JOB_SUCCESS=0
    comment: str     # Human readable string (eg error message)


class JobStatus(BaseModel):
    """
    Used by the 'read status' endpoint to return the status of a single job.

    job_id:       Integer ASVO job ID\n
    created:      Integer unix timestamp when the job was created\n
    completed:    True if all files have been staged\n
    total_files:  Total number of files in this job\n
    files:        Specifying the tuple type as tuple(bool, int) crashes the FastAPI doc generator\n

    The files attribute is a list of (ready:bool, readytime:int) tuples where ready_time is the time when that file
    was staged (or None)
    """
    job_id: int       # Integer ASVO job ID
    created: Optional[int] = None      # Integer unix timestamp when the job was created
    completed: Optional[bool] = None   # True if all files have been staged
    total_files: Optional[int] = None  # Total number of files in this job
    # The files attribute is a list of (ready:bool, readytime:int) tuples where ready_time
    # is the time when that file was staged (or None)
    files: dict[str, tuple] = {"":(False, 0)}     # Specifying the tuple type as tuple(bool, int) crashes the FastAPI doc generator


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
    kafkad_heartbeat: Optional[float] # Timestamp when kafkad.py last updated its status table
    kafka_alive: bool = False         # Is the remote kafka daemon alive?
    last_message: Optional[float]     # Timestamp when the last valid file status message was received
    completed_jobs: int = 0           # Total number of jobs with all files staged
    incomplete_jobs: int = 0          # Number of jobs waiting for files to be staged
    ready_files: int = 0              # Total number of files staged so far
    unready_files: int = 0            # Number of files we are waiting to be staged


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


def send_result(notify_url, job_id, ok, error_code=0, comment=''):
    """
    Call the given URL to report the success or failure of a job, passing a JobResult structure containing
    job_id:int, ok:bool, error_code:int and comment:str

    :param notify_url: URL to send job result to
    :param job_id: Integer job ID
    :param ok: Boolean - True means the job has succeeded in staging all files, False means there was an error
    :param error_code: Integer error code, eg JOB_SUCCESS=0
    :param comment: Human readable string (eg error messages)
    :return:
    """
    data = {'job_id':job_id,
            'ok':ok,
            'error_code':error_code,
            'comment':comment}
    result = requests.post(notify_url, json=data, auth=(config.RESULT_USERNAME, config.RESULT_PASSWORD))
    return result.status_code == 200


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
                    ok=False,
                    error_code=JOB_FILE_LOOKUP_FAILED,
                    comment='Failed to get files associated with the given observation.')
    elif not pathlist:
        send_result(notify_url=job.notify_url,
                    job_id=job.job_id,
                    ok=False,
                    error_code=JOB_NO_FILES,
                    comment='No files to stage.')
    else:
        try:
            with DB:
                data = {'path': pathlist, 'copy': 0, 'inode': []}
                result = requests.post(config.SCOUT_STAGE_URL, data=data, auth=ScoutAuth(get_scout_token()))
                ok = result.status_code == 200

                if ok:
                    with DB.cursor() as curs:
                        curs.execute(CREATE_JOB, (job.job_id,  # job_id
                                                  job.notify_url,  # URL to call on success/failure
                                                  datetime.datetime.now(timezone.utc),  # created
                                                  len(pathlist)))  # total_files
                        psycopg2.extras.execute_values(curs, WRITE_FILES, [(job.job_id, f, False) for f in job.files])
                else:
                    send_result(notify_url=job.notify_url,
                                job_id=job.job_id,
                                ok=False,
                                error_code=JOB_SCOUT_CALL_FAILED,
                                comment='Failed to contact the Scout API to stage the files.')
        except:
            send_result(notify_url=job.notify_url,
                        job_id=job.job_id,
                        ok=False,
                        error_code=JOB_CREATION_EXCEPTION,
                        comment='Exception during job creation: %s' % traceback.format_exc())


###############################################################################
# FastAPI endpoint definitions
#

app = FastAPI()


@app.post("/staging/",
          status_code=status.HTTP_201_CREATED,
          responses={403:{'model':ErrorResult}, 500:{'model':ErrorResult}, 502:{'model':ErrorResult}})
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
    try:
        with DB:
            with DB.cursor() as curs:
                curs.execute('SELECT count(*) FROM staging_jobs WHERE job_id = %s' % (job.job_id,))
                if curs.fetchone()[0] > 0:
                    response.status_code = status.HTTP_403_FORBIDDEN
                    err_msg = "Can't create job %d, because it already exists." % job.job_id
                    return ErrorResult(errormsg=err_msg)

        if (not job.files) or (not job.obs):
            err_msg = "Must specify either an observation, or a list of files"
            response.status_code = status.HTTP_400_BAD_REQUEST
            print(err_msg)
            return ErrorResult(errormsg=err_msg)

        background_tasks.add_task(create_job, job=job)
        return

    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        print(exc_str)
        return ErrorResult(errormsg='Exception creating staging job %d: %s' % (job.job_id, exc_str))


@app.get("/staging/",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobStatus}, 404:{'model':ErrorResult}, 500:{'model':ErrorResult}})
def read_status(job_id: int, response:Response):
    """
    GET API to read status details about an existing staging job. Accepts a single parameter in the
    URL (job_id, an integer), and looks up that job's data. The job status is returned as a
    JSON dict as defined by the JobStatus class.
    \f
    :param job_id:    # Integer ASVO job ID
    :param response:  # An instance of fastapi.Response(), used to set the status code returned
    :return:          # JSON dict as defined by the JobStatus() class above
    """
    try:
        with DB:
            with DB.cursor() as curs:
                curs.execute(QUERY_JOB, (job_id,))
                rows = curs.fetchall()
                if not rows:   # Job ID not found
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return ErrorResult(errormsg='Job %d not found' % job_id)
                if len(rows) > 1:   # Multiple rows for the same Job ID
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return ErrorResult(errormsg='Job %d has multiple rows!' % job_id)
                created, completed, total_files = rows[0]

                files = {}
                curs.execute(QUERY_FILES, (job_id,))
                for row in curs:
                    filename, ready, readytime = row
                    if readytime is not None:
                        readytime = int(readytime)
                    files[filename] = (ready, readytime)
                result = JobStatus(job_id=job_id,
                                   created=int(created),
                                   completed=completed,
                                   total_files=len(files),
                                   files=files)
                print("Job %d STATUS: %s" % (job_id, result))
                return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        print(exc_str)
        return ErrorResult(errormsg='Exception staging job %d: %s' % (job_id, exc_str))


@app.delete("/staging/",
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
    try:
        with DB:
            with DB.cursor() as curs:
                curs.execute(DELETE_JOB % (job_id,))
                if curs.rowcount == 0:
                    response.status_code = status.HTTP_404_NOT_FOUND
                    return ErrorResult(errormsg='Job %d not found' % job_id)
                curs.execute(DELETE_FILES, (job_id,))
                print('Job %d DELETED.' % job_id)
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        print(exc_str)
        return ErrorResult(errormsg='Exception staging job %d: %s' % (job_id, exc_str))


@app.get("/ping")
def ping():
    """
    GET API to return nothing, with a 200 status code, if we are alive. If we're not alive, this function won't be
    called at all.
    \f
    :return: None
    """
    return


@app.get("/get_stats",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':GlobalStatistics}, 500:{'model':ErrorResult}})
def get_stats(response:Response):
    """
    GET API to return an overall job and file statistics for this process (the staging web server)
    and the Kafka daemon (kafkad.py)
    \f
    :return: GlobalStatus object
    """
    try:
        with DB:
            with DB.cursor() as curs:
                curs.execute("SELECT update_time, last_message, kafka_alive FROM kafkad_heartbeat")
                kafkad_heartbeat, last_message, kafka_alive = curs.fetchall()[0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE completed")
                completed_jobs = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM staging_jobs WHERE completed")
                incomplete_jobs = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM files WHERE ready")
                ready_files = curs.fetchall()[0][0]
                curs.execute("SELECT count(*) FROM files WHERE not ready")
                unready_files = curs.fetchall()[0][0]

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
                                  unready_files=unready_files)
        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        print(exc_str)
        return ErrorResult(errormsg='Exception getting status: %s' % (exc_str,))


@app.get("/get_joblist",
         status_code=status.HTTP_200_OK,
         responses={200:{'model':JobList}, 500:{'model':ErrorResult}})
def get_joblist(response:Response):
    """
    GET API to return a list of all jobs, giving the number of staged files, the total number of files, and the
    completed status flag for each job.
    \f
    :return: JobList object
    """
    try:
        result = JobList()
        with DB:
            with DB.cursor() as curs:
                curs.execute(LIST_JOBS)
                for row in curs.fetchall():
                    job_id, staged_files, total_files, completed = row
                    result.jobs[job_id] = (staged_files, total_files, completed)

        return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        exc_str = traceback.format_exc()
        print(exc_str)
        return ErrorResult(errormsg='Exception getting status: %s' % (exc_str,))


@app.post("/v1/request/batchstage",
          status_code=status.HTTP_200_OK)
def dummy_scout_stage(path: Optional[list[str]] = Query(None), copy: int = 0, inode: Optional[list[int]] = Query(None)):
    """
    Emulate Scout's staging server for development. 'Stages' the files passed in the list of filenames.

    Always returns 200/OK and ignores the file list.
    \f
    :param path: A list of one or more filenames to stage
    :param inode: A list of one or more file inodes to stage
    :param copy: Integer - if 0, system will pick copy, otherwise use specified copy only
    :return: None
    """
    print("Pretending be Scout: Staging: %s" % (path,))


@app.get("/v1/file",
         status_code=status.HTTP_200_OK,
         response_model=ScoutFileStatus)
def dummy_scout_status(path: str = Query(...)):
    """
    Emulate Scout's staging server for development. Returns a status structure for the given (single) file.

    Returns a dummy status for the file specified - if the filename starts with 'offline_', then the status
    for the file has 1 block still offline, otherwise the status shows the file is online.
    \f
    :param path: Filename to check the status for. If it starts with 'offline_', then the returned status will be offline
    :return: None
    """
    resp = ScoutFileStatus(path=path)
    resp.onlineblocks = 22
    if path.startswith('offline_'):
        resp.offlineblocks = 1
    else:
        resp.offlineblocks = 0
    print("Pretending be Scout: Returning status for %s" % (path,))
    return resp


@app.post("/jobresult",
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
    print("Pretending to be ASVO: Job result received: %s" % (result,))
