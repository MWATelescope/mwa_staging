"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

from configparser import ConfigParser as conparser
import datetime
from datetime import timezone
from typing import Optional, List, Dict, Tuple

from fastapi import FastAPI, Response, status
import psycopg2
from psycopg2 import extras
from pydantic import BaseModel
import requests

# Real SCOUT URL - do not use for testing
# SCOUT_URL = 'http://192.168.0.40:8081/v1/request/batchstage'

# Dummy SCOUT URL - for testing:
SCOUT_URL = 'http://localhost'

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
INSERT INTO staging_jobs (job_id, created, completed, total_files)
VALUES (%s, %s, %s, %s)
"""

DELETE_JOB = """
DELETE FROM staging_jobs
WHERE job_id = %s
"""

WRITE_FILES = """
INSERT INTO files (job_id, filename, ready, readytime)
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


class Job(BaseModel):
    job_id: int       # Integer ASVO job ID
    files: List[str]  # A list of filenames, including full paths


class JobStatus(BaseModel):
    job_id: int       # Integer ASVO job ID
    created: int      # Integer unix timestamp when the job was created
    completed: bool   # True if all files have been staged
    total_files: int  # Total number of files in this job
    # The files attribute is a list of (ready:bool, readytime:int) tuples where ready_time
    # is the time when that file was staged (or None)
    files: Dict[str, Tuple[bool, int]]


def stage_files(job: Job):
    """
    Issues a POST request to the SCOUT API to stage the files given in 'job'

    :param job: An instance of the Job() class
    :return: None
    """
    post_data = {'path':job.files}
    requests.post(SCOUT_URL, data=post_data)


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
                if curs.fetchone[0] > 0:
                    response.status_code = status.HTTP_403_FORBIDDEN
                    return
                curs.execute(CREATE_JOB, (job.job_id,
                                          datetime.datetime.now(timezone.utc),
                                          False,
                                          len(job.files)))
                psycopg2.extras.execute_values(curs, WRITE_FILES, [(job.job_id, f) for f in job.files])

                try:
                    stage_files(job)   # Actually stage these files
                except Exception:  # Couldn't stage the files
                    DB.rollback()    # Reverse the job and file creation in the database
                    response.status_code = status.HTTP_502_BAD_GATEWAY
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


@app.get("/staging/{job_id}", response_model=Optional[JobStatus], status_code=status.HTTP_200_OK)
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
                created_datetime, completed, total_files = rows[0]
                created = int(created_datetime.timestamp())

                files = {}
                curs.execute(QUERY_FILES, (job_id,))
                for row in curs:
                    filename, ready, readytime_datetime = row
                    files[filename] = (ready, int(readytime_datetime.timestamp()))
                result = JobStatus(job_id=job_id,
                                   created=created,
                                   completed=completed,
                                   total_files=len(files),
                                   files=files)
                return result
    except Exception:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


@app.delete("/staging/{job_id}", status_code=status.HTTP_200_OK)
def delete_job(job_id: int, response:Response):
    """
    POST API to create a new staging job. Accepts a JSON dictionary defined above by the Job() class,
    which is processed by FastAPI and passed to this function as an actual instance of the Job() class.

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
        return
    except Exception:  # Any other errors
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
