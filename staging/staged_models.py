
#####################################################################
# Pydantic models defining FastAPI parameters or responses
#

from pydantic import BaseModel
from typing import Optional

from fastapi import Query

from mwa_files import MWAObservation as Observation


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


class FileStatus(BaseModel):
    """
    Used by the 'File status' endpoint to pass in the a list of files.

    files:       A list of filenames, including full paths\n
    """
    files: Optional[list[str]] = []   # A list of filenames, including full paths


class FileStatusResult(BaseModel):
    """
    Used by the 'File status' endpoint to return the status of the queried files

    allresults:   A dictionary with filename as key, and a boolean (True if the file is staged) as value
    """
    files: dict[str, bool] = {"":False}


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
    last_readytime: Integer unix timestamp of the last time a file was staged\n
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
    last_readytime: Optional[int] = None  # Integer unix timestamp of the last time a file was staged
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
    kafkad_heartbeat: Optional[float]   # Timestamp when kafkad.py last updated its status table
    kafka_alive: bool = False           # Is the remote kafka daemon alive?
    last_message: Optional[float]       # Timestamp when the last valid file status message was received
    completed_jobs: int = 0             # Total number of jobs with all files staged
    incomplete_jobs: int = 0            # Number of jobs waiting for files to be staged
    ready_files: int = 0                # Total number of files staged successfully so far
    error_files: int = 0                # Number of files for which Kafka reported an error
    waiting_files: int = 0              # Number of files we have still waiting to be staged
    availdatasz: Optional[int]          # scoutfs available data capacity
    health: Optional[int]               # scoutfs health of filesystem: 0-unknown, 1-ok, 2-warning, 3-error
    healthstatus: Optional[str]         # scoutfs health status info string
    releaseable: Optional[int]          # scoutfs capacity of online files eligible for release (archive complete)
    totdatasz: Optional[int]            # scoutfs total data capacity
    nonreleaseable: Optional[int]       # scoutfs non-releaseable data size
    pending: Optional[int]              # scoutfs size of data pending archiving


class JobList(BaseModel):
    """
    Used by the 'list all jobs' endoint to return a list of all jobs.

    jobs:   Key is job ID, value is (staged_files, total_files, completed)
    """
    jobs: dict[int, tuple] = {}     # Key is job ID, value is (job_id, created, staged_files, total_files, completed,
                                    # last_readytime, error_files)


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
