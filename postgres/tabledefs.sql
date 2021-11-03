
CREATE TABLE IF NOT EXISTS staging_jobs (
    -- One row per staging job
    job_id integer,
    created timestamp with time zone,
    completed boolean,
    total_files integer,
    notified boolean,
    checked boolean,

    PRIMARY KEY (job_id)
);

CREATE TABLE IF NOT EXISTS files (
    -- One row per file in ach job. The same filename in multiple jobs will be in multiple rows in this table.
    job_id integer,
    filename text,
    ready boolean,
    readytime timestamp with time zone,

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