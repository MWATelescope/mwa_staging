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

See [here](/Deployment) for more details on how to deploy this service.

## Each of the services explained

### API Server
As mentioned above, users of this staging service interact with it by sending 
web requests. Once you have the 'staged' service running, visit 
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
./postgres/tabledefs.sql.

### NGINX
We use NGINX as a simple proxy, which will direct incoming HTTPS connections 
from the outside world on port 443 to the FastAPI server running on port 
8000 (localhost only), implemented in the 'staged' service running 'staged.py' 
under uvicorn.

### HA Proxy
In the Banksia system, there are 6 so-called "vss" nodes which are running the 
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