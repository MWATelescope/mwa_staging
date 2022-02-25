"""
Library to handle turning an observation ID to a list of MWA files making up
that observation. Other telescopes using the staging code would substitute an alternate
library defining the same class and function, and import that from staged instead of
mwa_files.
"""

import os
from typing import Optional

from pydantic import BaseModel
import requests

DATA_FILES_URL = 'http://ws.mwatelescope.org/metadata/data_files'


class MWAObservation(BaseModel):
    """
    Used by the 'new job' endpoint to pass in an MWA obsid plus optional start time and duration. Note that if
    start_time and duration are specified for a non-VCS observation, then no files will be returned (the filenames
    have a different format). If start_time and duration are zero, they default to the beginning of the observation
    and 9999999999 respectively, for VCS observations.

    obs_id:      Obsid of the observation, used to look up the filenames\n
    start_time:  For VCS observations, when looking up filenames, ignore files before this time.\n
    duration:    For VCS observations, when looking up filenames, ignore files after start_time+duration\n
    """
    obs_id: Optional[int]           # Obsid of the observation, used to look up the filenames
    start_time: Optional[int] = 0   # For VCS observations, when looking up filenames, ignore files before this time.
    duration: Optional[int] = 0     # For VCS observations, when looking up filenames, ignore files after start_time+duration


def get_mwa_files(obs: MWAObservation):
    """
    Given an MWAObservation structure defining an MWA observation (obsid, and for VCS observations, a start time for
    the first file, and duration from that starttime), return a list of file names to stage.

    :param obs: Defines an MWA observation
    :return: A list of file names
    """
    if obs.start_time and obs.duration:
        mintime = obs.start_time
        maxtime = obs.start_time + obs.duration
    else:
        mintime = 0
        maxtime = 0
    data = {'obs_id': obs.obs_id,
            'mintime': mintime,
            'maxtime': maxtime,
            'terse': True,
            'all_files': False}
    result = requests.post(DATA_FILES_URL, data=data)

    if result.status_code != 200:
        return None

    pathlist = []
    for filename, filedata in result:
        pathlist.append(os.path.join(filedata['bucket'], filedata['folder'], filename))

    return pathlist
