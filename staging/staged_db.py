
"""
Database handling code and some query definitions, for staged.py
"""

from threading import Semaphore

from psycopg2 import pool


MINCONN = 2          # Start with this many database connections
MAXCONN = 100         # Don't ever create more than this many

DBPOOL = pool.ThreadedConnectionPool(0, 0)    # Will be a real, connected psycopg2 database connection pool object after startup


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


def initdb(config):
    """
    Initialise a connection pool to the database.

    :param config: A Config object containing database connection paramters
    :return: None
    """
    global DBPOOL
    DBPOOL = ReallyThreadedConnectionPool(minconn=MINCONN,
                                          maxconn=MAXCONN,
                                          host=config.DBHOST,
                                          user=config.DBUSER,
                                          database=config.DBNAME,
                                          password=config.DBPASSWORD)
