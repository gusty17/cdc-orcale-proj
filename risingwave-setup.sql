-- =====================================================================
-- RisingWave - consume the Debezium topic straight into a live table
-- =====================================================================
-- Connect with any Postgres client (RisingWave speaks the Postgres wire
-- protocol on 4566):
--   psql -h localhost -p 4566 -d dev -U root
--   -- or, if you don't have psql installed locally, run it from a
--   -- throwaway container on the same docker network:
--   docker run --rm --network cdc-oracle_default postgres:16-alpine \
--     psql -h risingwave -p 4566 -d dev -U root -f /dev/stdin < risingwave-setup.sql
--
-- CREATE TABLE (not CREATE SOURCE) is used so RisingWave actually stores
-- and maintains the data as a queryable changelog table, applying every
-- insert/update/delete it reads from Kafka - a plain CREATE SOURCE would
-- only let you query the raw stream, not a materialized current state.
--
-- FORMAT DEBEZIUM ENCODE JSON tells RisingWave to unwrap Debezium's
-- {op, before, after, source, ...} envelope automatically and apply each
-- change (insert/update/delete) against PRIMARY KEY (recid) - this is
-- why the connector's key.converter must actually emit {"RECID": "..."}
-- as the Kafka message key (see oracle-connector.json), since Debezium
-- format in RisingWave uses the key to identify which row to update.
-- =====================================================================

CREATE TABLE IF NOT EXISTS t24_account (
    recid     VARCHAR,
    xmlrecord VARCHAR,
    PRIMARY KEY (recid)
) WITH (
    connector = 'kafka',
    topic = 't24.T24.ACCOUNT',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE JSON;
