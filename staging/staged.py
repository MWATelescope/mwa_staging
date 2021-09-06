"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

from configparser import ConfigParser as conparser
import datetime
from datetime import timezone
import json
from typing import Optional, List, Dict, Tuple

import traceback
from fastapi import FastAPI, Response, status
import psycopg2
from psycopg2 import extras
from pydantic import BaseModel
import requests

# Real SCOUT URL - do not use for testing
# SCOUT_URL = 'http://192.168.0.40:8081/v1/request/batchstage'

# Dummy SCOUT URL - for testing:
SCOUT_URL = 'http://localhost:8000/v1/request/batchstage'

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

QUERY_FILES = """
SELECT filename, ready, extract(epoch from readytime)
FROM files
WHERE job_id = %s
"""


class JobResult(BaseModel):
    """
    Used to emulate the actual ASVO server during development.
    """
    job_id: int   # Integer ASVO job ID
    ok: bool      # True if the job succeeded, False if it failed.


class Job(BaseModel):
    job_id: int       # Integer ASVO job ID
    files: List[str]  # A list of filenames, including full paths


class JobStatus(BaseModel):
    job_id: int       # Integer ASVO job ID
    created: Optional[int] = None      # Integer unix timestamp when the job was created
    completed: Optional[bool] = None   # True if all files have been staged
    total_files: Optional[int] = None  # Total number of files in this job
    # The files attribute is a list of (ready:bool, readytime:int) tuples where ready_time
    # is the time when that file was staged (or None)
    files: Dict[str, Tuple[Optional[bool], Optional[int]]] = {}


def stage_files(job: Job):
    """
    Issues a POST request to the SCOUT API to stage the files given in 'job'

    :param job: An instance of the Job() class
    :return: None
    """
    # TODO - split into lots of individual requests to limit the number of filenames in the URL for each request
    post_data = {'path':job.files}
    result = requests.post(SCOUT_URL, data=json.dumps(post_data))
    return result.status_code == 200


app = FastAPI()


@app.post("/staging/", status_code=status.HTTP_201_CREATED)
def new_job(job: Job, response:Response):
    """
    POST API to create a new staging job. Accepts a JSON dictionary defined above by the Job() class,
    which is processed by FastAPI and passed to this function as an actual instance of the Job() class.

    :param job: An instance of the Job() class
    :param response: An instance of fastapi.Response(), used to set the status code returned
    :return: None
    """
    try:
        with DB:
            with DB.cursor() as curs:
                curs.execute('SELECT count(*) FROM staging_jobs WHERE job_id = %s' % (job.job_id,))
                if curs.fetchone()[0] > 0:
                    response.status_code = status.HTTP_403_FORBIDDEN
                    print("Can't create job %d, because it already exists." % job.job_id)
                    return
                curs.execute(CREATE_JOB, (job.job_id,                           # job_id
                                          datetime.datetime.now(timezone.utc),  # created
                                          len(job.files)))                      # total_files
                psycopg2.extras.execute_values(curs, WRITE_FILES, [(job.job_id, f, False) for f in job.files])

                ok = False
                try:
                    ok = stage_files(job)   # Actually stage these files
                except Exception:  # Couldn't stage the files
                    print(traceback.format_exc())

                if not ok:
                    print('Error contacting Scout to stage files for job %d, job not created.' % job.job_id)
                    DB.rollback()    # Reverse the job and file creation in the database
                    response.status_code = status.HTTP_502_BAD_GATEWAY
                else:
                    print('New job %d created with these files: %s' % (job.job_id, job.files))
        return
    except Exception:  # Any other errors
        print(traceback.format_exc())
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


@app.get("/staging/", status_code=status.HTTP_200_OK)  # , response_model=JobStatus)
def read_status(job_id: int, response:Response):
    """
    GET API to read status details about an existing staging job. Accepts a single parameter in the
    URL (job_id, an integer), and looks up that job's data. The job status is returned as a
    JSON dict as defined by the JobStatus class.

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
                    return
                if len(rows) > 1:   # Multiple rows for the same Job ID
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return
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
        print(traceback.format_exc())
        return


@app.delete("/staging/", status_code=status.HTTP_200_OK)
def delete_job(job_id: int, response:Response):
    """
    POST API to delete a staging job. Accepts an integer job_id value, to be deleted.

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
                    return
                curs.execute(DELETE_FILES, (job_id,))
                print('Job %d DELETED.' % job_id)
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


@app.post("/v1/request/batchstage{path}", status_code=status.HTTP_200_OK)
def dummy_scout_stage(path: str):  # TODO - find out how to handle multiple values
    """
    Emulate Scout's staging server for development. Always returns 200/OK and ignores the file list.

    :param path: One or more filenames to stage
    :return: None
    """
    print("Pretending be Scout: Staging: %s" % (path,))


@app.post("/v1/file{path}", status_code=status.HTTP_200_OK)
def dummy_scout_status(path: str):
    """
    Emulate Scout's staging server for development. Returns a dummy status for the file specified.

    :param path: Filename to check the status for.
    :return: None
    """
    print("Pretending be Scout: Returnin status for %s" % (path,))


@app.post("/jobresult", status_code=status.HTTP_200_OK)
def dummy_asvo(result: JobResult):
    """
    Emulate ASVO server for development. Always returns 200/OK.

    :param result: An instance of JobResult
    :return: None
    """
    print("Pretending to be ASVO: Job result received: %s" % (result,))
