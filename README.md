# MWA Staging

Software for staging MWA Telescope data from tape and onto disk where it can be 
later copied off.

MWA Telescope data is stored across two systems, Acacia and Banksia, both of 
which are located at the Pawsey Supercomputing Centre in Perth.

While Acacia is a disk-based system, and all data is immediately available for 
copy, Banksia uses a robotic tape library for data archival, and data must be 
copied onto a disk cache before it can be accessed.

Banksia provides a Rest API for interacting with the system, an S3 compliant 
gateway for copying data, and sends messages via Apache Kafka as each file 
is staged off tape onto the gateway. This software handles only the staging of 
data by interacting with the REST API and listening to the Kafka messages, 
and is not responsible for copying data off the gateway. 

This software presents a web service where a user can provide either an MWA 
Observation ID (obs_id) or list of files, as well as a callback URL, and some 
other parameters. Once staging is completed, a request is sent to the specified 
callback URL to indicate that all files have been staged and are ready to 
be copied, or to pass on any errors.

## High Level Architecture
Requests are received via https (443) requests to an NGINX web server, which will 
proxy these requests to a Python FastAPI web server listening (only on 
localhost) on port 8000. Once a 'new staging job' request has been received
(containing either an MWA observation ID or file list), the FastAPI code will
do some basic sanity checking, then return immediately with a 202 ('Accepted') 
status code. 

The 'new job' endpoint spawns a background thread to do the actual work of 
looking up the list of files, calling the Banksia API to stage those files, and 
creating the new job in the PosgreSQL database. That background thread first 
uses the observation ID to look up the files in the main MWA Telescope database 
if necessary (using an MWA web service), then calls the Scout API to stage those 
files, and finally creates records in the Postgres database describing the
new staging job.

In the production Banksia system, there are 6 so-called "vss" nodes which 
are running the Scout API. We therefore use HA Proxy on the client side to 
load balance requests between each of these nodes. HAProxy is set to listen 
on http://localhost:8080, and redistribute any calls to that address to one of the
six Scout API servers on 146.118.74.144-149 on port 8080, rotating through 
the six servers to balance the load and work around individual server outages.

As files are being copied off of tape and onto cache, Banksia will publish 
messages to a Kafka message bus, one for each file, containing the name of
that file. The kafkad service connects to Kafka on boot, and as messages come 
through, it will mark each file as being staged. A separate thread in the kafkad 
service continuously monitors the database and checks if all the files for a 
particular request are "done" and if so, it will send a request to the specified 
callback URL notifying it of this. Once the caller has been successfully notified
that the job is complete (or has timed out), the job will be deleted from the
database.

## Installation
Assuming a vanilla Ubuntu 24.04 installation:

  - Install packages and create the staging user, as root:
```
apt install direnv haproxy postgresql python3-pip python-is-python3 python3.12-venv nginx
adduser --disabled-password staging
mkdir /var/log/staging
chown staging:staging /var/log/staging
```
 
  - Download and set up the software, as the staging user:
    - [Optional]: Run ssh-keygen to create an SSH key, and copy the SSH key to github 'mwa-site' user
    - Install this line at the end of ~/.bashrc:
      ```
      eval "$(direnv hook bash)"
      ```
    - Log out and back in again (or otherwise force the new .bashrc to be loaded)
    - Download the git repo:
      ```
      git clone git@github.com:MWATelescope/mwa_staging.git
      ```
    - Create  mwa_staging/.envrc and add all the environment variables as described below,
      using the template mwa_staging/config_file_examples/dot_envrc.example

    - Install the Python environment and dependencies, as the staging user:
      ```
      cd mwa_staging
      direnv allow     # You should see a message about all the environment variables being set
      
      python -m venv staging_env
      source staging_env/bin/activate
      
      pip install orjson
      pip install "psycopg[binary]"
      pip install psycopg_pool
      pip install "fastapi[all]"
      pip install kafka-python
      pip install requests
      ```



  - Set up haproxy, as root:
```
cp /home/staging/mwa_staging/config_file_examples/haproxy.cfg /etc/haproxy.cfg
systemctl restart haproxy
```

  - Set up nginx, as root:
```
rm /etc/nginx/sites-enabled/default
cp /home/staging/mwa_staging/config_file_examples/nginx.conf.example /etc/nginx/conf.d/default.conf

systemctl restart nginx
```

  - Set up PostgreSQL, as root:
    - Default config files in /etc/postgres can be left unchanged (listening only on 127.0.0.1)
    ```
    sudo su postgres
    psql
    postgres-# CREATE USER staging WITH ENCRYPTED PASSWORD '<insert_secret_here>';
    postgres=# CREATE DATABASE staging;
    postgres=# GRANT ALL PRIVILEGES ON DATABASE staging TO staging;
    ```
    - Then exit psql (Ctrl-D), and run:
    ```
    psql -d staging
    postgres=# GRANT ALL ON SCHEMA public TO staging
    ```
    - Then exit psql again (Ctrl-D) and run:
    ```
    su -l staging
    psql -d staging
    ```
    - Then paste the contents of /home/staging/mwa_staging/config_file_examples/tabledefs.sql 
     into the psql prompt.


  - Set up systemd services for staged and kafkad, and start the software, as root:
    ```
    cp config_file_examples/kafkad.service.example /etc/systemd/system/kafkad.service
    cp config_file_examples/staged.service.example /etc/systemd/system/staged.service
    
    systemctl daemon-reload
    systemctl enable kafkad.service staged.service
    systemctl start kafkad.service staged.service
    ```

### Environment Variables
We make use of environment variables to store configuration information. A 
template containing the required variables is included below. We recommend 
using [direnv](https://direnv.net/) to manage this config.
```bash
# Credentials to pass to the Scout API to stage files and request file status
export SCOUT_API_USER=<Insert secret here>
export SCOUT_API_PASSWORD=<Insert secret here>

# The Scout API endpoints to stage files and request file status.
export SCOUT_LOGIN_URL=https://localhost:8080/v1/security/login
export SCOUT_QUERY_URL=https://localhost:8080/v1/file
export SCOUT_STAGE_URL=https://localhost:8080/v1/request/batchstage
export SCOUT_CANCEL_URL=https://localhost:8080/v1/request/cancelbatchstage
export SCOUT_CACHESTATE_URL=https://localhost:8080/v1/fscache

# Note that tha staged.py API includes dummy versions of some of these endpoints
# for testing - use a prefix of http://localhost:8000/ for this.
#export SCOUT_LOGIN_URL=http://localhost:8000/v1/security/login
#export SCOUT_QUERY_URL=http://localhost:8000/v1/file
#export SCOUT_STAGE_URL=http://localhost:8000/v1/request/batchstage

# The Basic Auth credentials to pass to the 'notify_url' given when a job is created
export RESULT_USERNAME=<Insert secret here>
export RESULT_PASSWORD=<Insert secret here>

# PostgreSQL database parameters for maintaining the current job and file lists
export DBUSER=staging
export DBPASSWORD=<Insert secret here>
export DBHOST=localhost
export DBNAME=staging

# Kafka server credentials
export KAFKA_TOPIC=<Insert secret here>
export KAFKA_SERVER=franz.pawsey.org.au:9093
export KAFKA_USER=<Insert secret here>
export KAFKA_PASSWORD=<Insert secret here>

# Web service to look up what files are associated with a given MWA observation ID
export DATA_FILES_URL=http://ws.mwatelescope.org/metadata/data_files

# Log file directory
export STAGING_LOGDIR=/var/log/staging

# How many days to keep old job failure reports in STAGING_LOGDIR/failed/<job_id>.txt
export REPORT_EXPIREDAYS=30

# Re-stage interval, for any files that we are still waiting on
export FILE_RESTAGE_INTERVAL=21600

# Job expiry time (how long to wait for files before we give up and notify ASVO that it has failed
export JOB_EXPIRY_TIME=691200

```


## Each of the services explained

### API Server
As mentioned above, users of this staging service interact with it by sending 
web requests. Once you have the local stack running, visit 
(http://localhost:8080/docs). This page is generated automatically by FastAPI 
and includes comprehensive documentation of each of the endpoints and methods 
provided by the FastAPI service. It also allows you to send test requests and 
view their output.

The API server is implemented in staged.py, mwa_files.py, staged_models.py and 
staged_db.py.

All the MWA-specific code and data structures are in mwa_files.py, to make
it easier to adapt this code for a different telescope.

### PostgreSQL & PG Admin
We make use of a local PostgreSQL server to store records related to staging 
jobs and their associated files. You can view the table definitions in 
./postgres/tabledefs.sql. Once your stack (development, not production) 
stack is running, you can visit http://localhost:5050 to access the locally 
running PG Admin, which will provide a UI to view and interact with the 
database. 

### NGINX
We use NGINX as a reverse proxy, which will direct traffic to the various 
services.

### HA Proxy
This is included in the development stack in order to mirror a production 
environment as closely as possible. As mentioned above, in the production 
Banksia system, there are 6 so-called "vss" nodes which are running the 
API. We therefore use HA Proxy on the client side to load balance requests 
between each of these nodes. This will handle round-robin between servers, 
and auto-retries if one of them is down.

### Kafkad
As files are being copied off of tape and onto cache, Banksia will publish 
messages to a Kafka message bus. The kafkad service connects to Kafka on 
boot, and as messages come through, it will mark each file as being staged. 
A separate thread in the kafkad service continuously monitors the database 
and checks if all the files for a particular request are "done" and if 
so, it will send a request to the specified callback URL notifying it of 
this.

The kafka message handler is implemented in kafkad.py.