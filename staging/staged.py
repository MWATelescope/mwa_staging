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
from fastapi import FastAPI, Query, Response, status
import psycopg2
from psycopg2 import extras
from pydantic import BaseModel
import requests
from requests.auth import AuthBase


class ApiConfig():
    def __init__(self):
        self.DBUSER = os.getenv('DBUSER')
        self.DBPASSWORD = os.getenv('DBPASSWORD')
        self.DBHOST = os.getenv('DBHOST')
        self.DBNAME = os.getenv('DBNAME')
        self.SCOUT_STAGE_URL = os.getenv('SCOUT_STAGE_URL')
        self.SCOUT_API_TOKEN = os.getenv('SCOUT_API_TOKEN')


config = ApiConfig()

DB = psycopg2.connect(user=config.DBUSER,
                      password=config.DBPASSWORD,
                      host=config.DBHOST,
                      database=config.DBNAME)

CREATE_JOB = """
INSERT INTO staging_jobs (job_id, created, completed, total_files, notified)
VALUES (%s, %s, false, %s, false)
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

#####################################################################
# Pydantic models defining FastAPI parameters or responses
#


class JobResult(BaseModel):
    """
    Used by the dummy ASVO server endpoint to pass in the results of a job once all files are staged.
    """
    job_id: int   # Integer ASVO job ID
    ok: bool      # True if the job succeeded, False if it failed.


class Job(BaseModel):
    """
    Used by the 'new job' endpoint to pass in the job ID and list of files.
    """
    job_id: int       # Integer ASVO job ID
    files: list[str]  # A list of filenames, including full paths


class JobStatus(BaseModel):
    """
    Used by the 'read status' endpoint to return the status of a single job.
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
    """
    jobs: dict[int, tuple] = {}  # Key is job ID, value is (staged_files, total_files, completed)


class ErrorResult(BaseModel):
    """
    Used by all jobs, to return an error message if the call failed.
    """
    errormsg: str   # Error message returned to caller


class ScoutFileStatus(BaseModel):
    """
    Used by the Scout API dummy status endpoint, to return the status of a single file.
    """
    archdone: bool = True     # All copies complete, file exists on tape (possibly on disk as well)
    path: str                 # First file name for file, for policy decisions, eg "/object.dat.0",
    size: int = 0             # File size in bytes, eg "10485760",
    inode: int = 0            # File inode number, eg "2",
    uid: int = 0              # User ID for file, eg "0",
    username: str = ""        # User name for file, eg "root",
    gid: int = 0              # group ID for file, eg "0",
    groupname: str = ""       # group name for file, eg "root",
    mode: int = 0             # file mode, eg 33188,
    modestring: str = ""      # file mode string, eg "-rw-r--r--",
    nlink: int = 0            # number of links to file, eg 1,
    atime: int = 0            # access time, eg "1618411999",
    mtime: int = 0            # modify time, eg "1618411968",
    ctime: int = 0            # attribute change time, eg "1618415117",
    rtime: int = 0            # residence time, eg "1618415102",
    version: str = ""         # Data version, eg "2560",
    onlineblocks: int = 0     # Number of online blocks, eg "2560",
    offlineblocks: int = 0    # Number of offline blocks, eg "0",
    flags: int = 0            # archive flags, eg "0",
    flagsstring: str = ""     # archive flags string, eg "",
    uuid: str = ""            # UUID for file, eg "8e9825f9-291d-415e-bb1f-38d8f865dcac",
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


def stage_files(job: Job):
    """
    Issues a POST request to the SCOUT API to stage the files given in 'job'.

    :param job: An instance of the Job() class
    :return: None
    """
    data = {'path':job.files, 'copy':0, 'inode':[]}
    result = requests.post(config.SCOUT_STAGE_URL, data=data, auth=ScoutAuth(config.SCOUT_API_TOKEN))
    return result.status_code == 200


###############################################################################
# FastAPI endpoint definitions
#

app = FastAPI()


@app.post("/staging/",
          status_code=status.HTTP_201_CREATED,
          responses={403:{'model':ErrorResult}, 500:{'model':ErrorResult}, 502:{'model':ErrorResult}})
def new_job(job: Job, response:Response):
    """
    POST API to create a new staging job. Accepts a JSON dictionary defined above by the Job() class,
    which is processed by FastAPI and passed to this function as an actual instance of the Job() class.
    \f
    :param job: An instance of the Job() class. Job.job_id is the new job ID, Job.files is a list of file names.
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
                curs.execute(CREATE_JOB, (job.job_id,                           # job_id
                                          datetime.datetime.now(timezone.utc),  # created
                                          len(job.files)))                      # total_files
                psycopg2.extras.execute_values(curs, WRITE_FILES, [(job.job_id, f, False) for f in job.files])

                ok = False
                exc_str = ''
                try:
                    ok = stage_files(job)   # Actually stage these files
                except Exception:  # Couldn't stage the files
                    exc_str = traceback.format_exc()
                    print(exc_str)

                if not ok:
                    err_msg = 'Error contacting Scout to stage files for job %d, job not created: %s' % (job.job_id, exc_str)
                    DB.rollback()    # Reverse the job and file creation in the database
                    response.status_code = status.HTTP_502_BAD_GATEWAY
                    print(err_msg)
                    return ErrorResult(errormsg=err_msg)
                else:
                    print('New job %d created with these files: %s' % (job.job_id, job.files))
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
    Emulate Scout's staging server for development. Always returns 200/OK and ignores the file list.
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
    Emulate Scout's staging server for development. Returns a dummy status for the file specified.
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
    Emulate ASVO server for development. Always returns 200/OK.
    \f
    :param result: An instance of JobResult
    :return: None
    """
    print("Pretending to be ASVO: Job result received: %s" % (result,))
