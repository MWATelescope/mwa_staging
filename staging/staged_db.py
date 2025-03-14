
"""
Database handling code and some query definitions, for staged.py
"""

from threading import Semaphore

import psycopg_pool


MINCONN = 2          # Start with this many database connections
MAXCONN = 100         # Don't ever create more than this many

DBPOOL = None    # Will be a real, connected psycopg2 database connection pool object after startup


CREATE_JOB = """
INSERT INTO staging_jobs (job_id, notify_url, created, completed, total_files)
VALUES (%s, %s, %s, false, %s)
"""

DELETE_JOB = """
DELETE FROM staging_jobs
WHERE job_id = %s
"""

WRITE_FILES = """
INSERT INTO files (job_id, filename, ready, error) 
VALUES (%s, %s, %s, %s)
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

LIST_JOBS_NEW = """
SELECT sj.job_id as job_id, 
       extract(epoch from sj.created) as created,
       (SELECT count(*) FROM files WHERE files.job_id=sj.job_id AND ready) as staged_files, 
       sj.total_files as total_files, 
       sj.completed as completed, 
       (SELECT extract(epoch from max(f.readytime)) FROM files as f WHERE f.job_id=sj.job_id) as last_readytime,
       (SELECT count(*) FROM files WHERE files.job_id=sj.job_id AND error) as error_files
FROM staging_jobs as sj
ORDER BY sj.job_id;
"""

QUERY_FILES = """
SELECT filename, ready, error, extract(epoch from readytime)
FROM files
WHERE job_id = %s
"""

QUERY_LAST_READY = """
SELECT extract(epoch from max(readytime))
FROM files
WHERE job_id = %s and ready
"""

# All files belonging to the given job_id, that AREN'T in any other job
QUERY_UNIQUE_FILES = """
SELECT filename 
FROM (SELECT filename 
      FROM files 
      WHERE job_id=%s) AS tquery 
WHERE (SELECT count(*) 
       FROM files 
       WHERE files.filename=tquery.filename AND job_id <> %s) = 0
"""


def initdb(config):
    """
    Initialise a connection pool to the database.

    :param config: A Config object containing database connection paramters
    :return: None
    """
    global DBPOOL
    DBPOOL = psycopg_pool.ConnectionPool('postgresql://%s:%s@%s/%s' % (config.DBUSER, config.DBPASSWORD, config.DBHOST, config.DBNAME),
                                         min_size=MINCONN,
                                         max_size=MAXCONN,
                                         open=True,
                                         timeout=360,)
