CREATE KEYSPACE IF NOT EXISTS nasa_logs
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE nasa_logs;

CREATE TABLE IF NOT EXISTS requests_by_endpoint (
    endpoint text PRIMARY KEY,
    count counter
);

CREATE TABLE IF NOT EXISTS requests_by_status (
    status_code int PRIMARY KEY,
    count counter
);

CREATE TABLE IF NOT EXISTS requests_by_ip (
    ip_address text PRIMARY KEY,
    count counter
);

CREATE TABLE IF NOT EXISTS logs_raw (
    uuid uuid PRIMARY KEY,
    ip_address text,
    endpoint text,
    status_code int,
    log_time timestamp
);
