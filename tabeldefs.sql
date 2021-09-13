
CREATE TABLE staging_jobs (
    -- One row per staging job
    job_id integer,
    created timestamp with time zone,
    completed boolean,
    total_files integer,
    notified boolean,
    checked boolean,

    PRIMARY KEY (job_id)
);

CREATE TABLE files (
    -- One row per file in ach job. The same filename in multiple jobs will be in multiple rows in this table.
    job_id integer,
    filename text,
    ready boolean,
    readytime timestamp with time zone,

    PRIMARY KEY (job_id, filename)
);

CREATE INDEX files_filename ON files (filename);
