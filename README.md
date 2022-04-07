# MWA Staging

Software for staging MWA Telescope data from tape and onto disk where it can be 
later copied off.

MWA Telescope data is stored across two systems, Acacia and Banksia, both of 
which are located at the Pawsey Supercomputing Centre in Perth.

While Acacia is a disk-based system, and all data is immediately available for 
copy, Banksia uses a robotic tape library for data archival, and data must be 
copied onto a disk cache before it can be accessed.

Banksia provides a Rest API for interacting with the system, and an S3 compliant 
gateway for copying data. This software handles only the staging of data by 
interacting with the REST API, and is not responsible for copying data. This 
software presents a web service where a user can provide either an MWA 
Observation ID (obs_id) or list of files, a callback URL, and some other 
parameters. Once staging is completed, a request is sent to the specified 
callback URL to indicate that all files have been staged and are ready to 
be copied.

We use two different sets of Docker containers, one for development, and 
another for production. These can be started easily by using the included 
compose file.

## High Level Architecture
When the stack is running, an NGINX web server will proxy requests to a Python 
FastAPI web server. Once a 'new staging job' request has been received
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
are running the API. We therefore use HA Proxy on the client side to load balance 
requests between each of these nodes.

As files are being copied off of tape and onto cache, Banksia will publish 
messages to a Kafka message bus, one for each file, containing the name of
that file. The kafkad service connects to Kafka on boot, and as messages come 
through, it will mark each file as being staged. A separate thread in the kafkad 
service continuously monitors the database and checks if all the files for a 
particular request are "done" and if so, it will send a request to the specified 
callback URL notifying it of this.

## Installation
- Download the repository
- Set environment variables
- Create the log directory (/var/log/staging) and make it writeable by the process
  running the docker stack.
- Bring up the stack

### Environment Variables
We make use of environment variables to store configuration information. A 
template containing the required variables is included below. We recommend 
using [direnv](https://direnv.net/) to manage this config.
```bash
# Credentials to pass to the Scout API to stage files and request file status
export SCOUT_API_USER=
export SCOUT_API_PASSWORD=

# The Scout API endpoints to stage files and request file status.
# Note that tha staged.py API includes dummy versions of these endpoints
# for testing - use a prefix of http://localhost:8000/ for this.
export SCOUT_LOGIN_URL=https://haproxy:8081/v1/security/login
export SCOUT_QUERY_URL=https://haproxy:8081/v1/file
export SCOUT_STAGE_URL=https://haproxy:8081/v1/request/batchstage
export SCOUT_CANCEL_URL=https://haproxy:8080/v1/request/cancelbatchstage

# The Basic Auth credentials to pass to the 'notify_url' given when a job is created
export RESULT_USERNAME=
export RESULT_PASSWORD=

# PostgreSQL database parameters for maintaining the current job and file lists
export DBUSER=
export DBPASSWORD=
export DBHOST=
export DBNAME=

# Kafka server credentials
export KAFKA_TOPIC=
export KAFKA_SERVER=
export KAFKA_USER=
export KAFKA_PASSWORD=

# Web service to look up what files are associated with a given MWA observation ID
export DATA_FILES_URL=http://ws.mwatelescope.org/metadata/data_files
```

### Starting the development stack
Once the config info has been set, use the docker-compose.yml file to bring up 
the development stack.

Note that the development stack will require a different set of environment 
variables pointing to the local (test) Kafka server, dummy Banksia endpoints 
defined in staged.py, etc.

Note also that the development stack hasn't had a lot of testing, and is NOT safe to 
run in at the same time as the production stack - for example, both use the same
physical volume to store the PostgreSQL database files.
```bash
docker-compose up
```

### Starting the production stack
Once the config info has been set, use the docker-compose-prod.yml file to bring 
up the production stack.
```bash
docker-compose -f docker-compose-prod.yml up --detach
```

## Services included with each stack
Both the development and production stacks use a different, and overlapping set 
of services, which are listed below.

### Development
The development stack includes extra services to facilitate local development 
and testing. Including an instance of Kafka (which requires Zookeeper) and PG 
Admin for administration of the included PostgreSQL database.

- API Server (staging/staged.py)
- PostgreSQL Database
- PG Admin
- NGINX
- Zookeeper
- Kafka
- kafkad (staging/kafkad.py)
- HA Proxy

### Production
- API Server (staging/staged.py)
- PostgreSQL Database
- NGINX
- kafkad (staging/kafkad.py)
- HA Proxy

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

### PostgreSQL & PG Admin
We make use of a local PostgreSQL server to store records related to staging 
jobs and their associated files. You can view the table definitions in 
./postgres/tabledefs.sql. Once your stack is running, you can visit 
http://localhost:5050 to access the locally running PG Admin, which will 
provide a UI to view and interact with the database. 

### NGINX
We use NGINX as a reverse proxy, which will direct traffic to the various 
services.

### Kafka & ZooKeeper
To facilitate local development, the development stack inludes an instance 
of Kafka (for which Zookeeper is required) which will allow you to manually 
send staging messages and verify that they are being read and associated 
rows being marked correctly. This has not been well tested, and you may need 
to refer to the [docs](https://hub.docker.com/r/wurstmeister/kafka) for the 
Kafka container to configure this properly.

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