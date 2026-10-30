# Overview:

Implements the 'staged' service, controlled by systemd:

`systemctl start|stop|restart staged`

This is the FastAPI web server that implements the staged service. It provides a number of 
endpoints used by ASVO to request that the files associated with MWA observations be 
staged off tape onto an Acacia 'S3' style filesystem for downloading. 

It also has endpoints that allow ASVO to monitor the status of the staged service, and to 
query the database for the status of files that have been staged. These are described in 
the [Logging and Monitoring](Logging_and_monitoring.md) section.

Some additional endpoints are included for testing - these emulate the Scout API endpoints that are 
used to stage files, and a dummy callback URL that can be used instead of a real ASVO callback.

## Code documentation:

The main code is implemented in [staged.py](staged_py.md).

It uses the following modules:

* [staged_db.py](staged_db_py.md) - defines SQL queries for the database interface
* [staged_models.py](staged_models_py.md) - defines the PyDantic models for the API.
