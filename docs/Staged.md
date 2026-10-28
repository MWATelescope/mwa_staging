# FastAPI web server:

## PyDantic models

The PyDantic models are used to validate and parse the JSON data received by the
web server. They are defined in staged_models.py.

::: staging.staged_models

## SQL queries

The SQL queries are used to interact with the PostgreSQL database. They are defined in 
staged_db.py.

::: staging.staged_db

## Main staging server

The main staging server is implemented in staged.py.

::: staging.staged