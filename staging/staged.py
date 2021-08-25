"""
Staging daemon, to take requests for files from ASVO, request those files from the tape system, listen for Kafka
messages about those files, and return the results to ASVO.

This file implements the REST API for accepting requests from ASVO, and another process will handle Kafka messages.
"""

from configparser import ConfigParser as conparser
from typing import List, Optional

from fastapi import FastAPI
import psycopg2
from pydantic import BaseModel


CPPATH = ['/usr/local/etc/pasd.conf', '/usr/local/etc/pasd-local.conf',
          './pasd.conf', './pasd-local.conf']
CP = conparser(defaults={})
CPfile = CP.read(CPPATH)
if not CPfile:
    print("None of the specified configuration files found by mwaconfig.py: %s" % (CPPATH,))

dbuser = CP['dbuser']
dbhost = CP['dbhost']
dbpass = CP['dbpass']
dbname = CP['dbname']

db = psycopg2.connect(user=CP['dbuser'], password=CP['dbpass'], host=CP['dbhost'], database=dbname)


class Job(BaseModel):
    job_id: int
    files: List[str]


app = FastAPI()


@app.post("/stage/")
def new_job(job: Job):
    return {"result":"Job %d created." % job.job_id}


@app.get("/stage/{job_id}")
def read_status(job_id: int):
    return {"job_id": job_id}

