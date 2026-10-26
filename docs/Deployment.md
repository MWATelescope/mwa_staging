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
