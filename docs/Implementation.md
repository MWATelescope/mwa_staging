# Implementation

The staging service code is made up of:
 
- A FastAPI web server (staged.py) responding to staging requests and creating new jobs,
  running behind NGINX.
- A daemon (kafkad.py) that listens for Kafka messages from Scout as
each file is staged, then notifies ASVO when a job is complete (or fails) by 
calling the provided callback URL. 

The web server is implemented in staged.py, mwa_files.py, staged_models.py and 
staged_db.py, and the code is described in detail here:

[Staged](Staged)

The Kafka daemon is implemented in kafkad.py, and the code is described in detail here:

[Kafkad](Kafkad)
