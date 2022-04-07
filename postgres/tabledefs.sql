
CREATE TABLE IF NOT EXISTS staging_jobs (
    -- One row per staging job
    job_id integer,
    notify_url text,                   -- Callback URL for when a job is completed, or times out
    created timestamp with time zone,
    completed boolean,                 -- True if all files have been staged
    total_files integer,
    notified boolean,                  -- Not used in the current version
    checked boolean,                   -- Not used in the current version

    PRIMARY KEY (job_id)
);

CREATE TABLE IF NOT EXISTS files (
    -- One row per file in ach job. The same filename in multiple jobs will be in multiple rows in this table.
    job_id integer,
    filename text,
    ready boolean,                       -- True if the file has been staged successfully
    error boolean,                       -- True if the Kafka message indicated an error staging that file
    readytime timestamp with time zone,  -- Timestamp when the Kafka message about that file (ready or error) was processed.

    PRIMARY KEY (job_id, filename)
);

CREATE INDEX IF NOT EXISTS files_filename ON files (filename);

CREATE TABLE IF NOT EXISTS kafkad_heartbeat (
    -- Updated every few seconds if the Kafka Daemon (kafkad.py) is running
    update_time timestamp with time zone,
    last_message timestamp with time zone,
    kafka_alive boolean
);

-- Delete stale table contents, and insert a single status row, ready for software startup
DELETE FROM kafkad_heartbeat WHERE true;
INSERT INTO kafkad_heartbeat (update_time, last_message, kafka_alive) VALUES (NULL, NULL, false);