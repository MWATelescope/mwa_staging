# Logging and Monitoring

## Monitoring endpoints

There are number of FastAPI endoints that allow external services to monitor the status of the 
staging service. These include:

* `/ping` - returns a 200 OK response if the staged service is running
* `/get_stats` - returns a JSON structure with the current state of the staged and 
  kafkad services, as well as health data from Scout.
* `/get_joblist` - returns a JSON structure with the current job list for the staged service.
* `/get_failed_joblist` - returns a JSON structure with a list of all jobs that have recently 
   failed to stage.
* `/get_failed_job` - given a recently failed job ID, returns a JSON structure with the details 
   of that job.
* `/file_status` - given a JSON structure containing a list of filenames, return another JSON 
   structure with the status of each file.


## Logging

The staged service, implemented in staged.py, logs to /var/log/staging/staged.log. The 
kafkad service, implemented in kafkad.py, logs to /var/log/staging/kafkad.log. Log files 
are rotated when they reach 100 MB, and the most recent 5 rotations are kept.

In addition, recent failed jobs (as defined by the REPORT_EXPIREDAYS environment variable)
have a failure report written to `/var/log/staging/failed/<jobid>.txt`. It is the contents
of this directory that are returned by the `get_failed_joblist` and
`/get_failed_job` endpoints.