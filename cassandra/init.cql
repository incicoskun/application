CREATE KEYSPACE IF NOT EXISTS nasa_logs
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE nasa_logs;

CREATE TABLE IF NOT EXISTS requests_by_endpoint (
    log_time timestamp,
    endpoint text,
    count int,
    PRIMARY KEY (log_time, endpoint)
);

CREATE TABLE IF NOT EXISTS requests_by_status (
    log_time timestamp,
    status_code int,
    count int,
    PRIMARY KEY (log_time, status_code)
);

CREATE TABLE IF NOT EXISTS requests_by_ip (
    log_time timestamp,
    ip_address text,
    count int,
    PRIMARY KEY (log_time, ip_address)
);

CREATE TABLE IF NOT EXISTS logs_raw (
    uuid uuid PRIMARY KEY,
    ip_address text,
    endpoint text,
    status_code int,
    log_time timestamp
);
