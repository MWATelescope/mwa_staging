"""
Library to handle turning an observation ID to a list of MWA files making up
that observation. Other telescopes using the staging code would substitute an alternate
library defining the same class and function, and import that from staged instead of
mwa_files.
"""


from typing import Optional

from pydantic import BaseModel
import requests


class NewMWAJob(BaseModel):
    """
    Used by the 'new job' endpoint to pass in the job ID and either a list of files, or an MWA obsid plus
    optional start time and duration. Note that if start_time and duration are specified for a non-VCS observation,
    then no files will be returned (the filenames have a different format). If start_time and duration are zero,
    they default to the beginning of the observation and 9999999999 respectively, for VCS observations.

    Note that job_id and files are used externally in staged.py, so must be present in any other version of the NewJob
    structure, while obs_id, start_time and duration are used only here in mwa_files.py.

    job_id:      Integer ASVO job ID\n
    files:       A list of filenames, including full paths\n
    obs_id:      Obsid of the observation, used to look up the filenames\n
    start_time:  For VCS observations, when looking up filenames, ignore files before this time.\n
    duration:    For VCS observations, when looking up filenames, ignore files after start_time+duration\n
    """
    job_id: int                     # Integer ASVO job ID
    files: Optional[list[str]]      # A list of filenames, including full paths
    obs_id: Optional[int]           # Obsid of the observation, used to look up the filenames
    start_time: Optional[int] = 0   # For VCS observations, when looking up filenames, ignore files before this time.
    duration: Optional[int] = 0     # For VCS observations, when looking up filenames, ignore files after start_time+duration



