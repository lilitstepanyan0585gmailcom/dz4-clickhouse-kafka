CREATE DATABASE IF NOT EXISTS transactions_db;
USE transactions_db;

DROP TABLE IF EXISTS transactions_kafka;
CREATE TABLE transactions_kafka
(
  transaction_time String,
  merch String,
  cat_id String,
  amount Float64,
  name_1 String,
  name_2 String,
  gender String,
  us_state String,
  lat Float64,
  lon Float64,
  merchant_lat Float64,
  merchant_lon Float64,
  target UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'transactions',
  kafka_group_name = 'ch_consumer',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions
(
  transaction_time DateTime,
  merch String,
  cat_id LowCardinality(String),
  amount Float64,
  name_1 String,
  name_2 String,
  gender LowCardinality(String),
  us_state LowCardinality(String),
  lat Float64,
  lon Float64,
  merchant_lat Float64,
  merchant_lon Float64,
  target UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, transaction_time)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS mv_transactions_to_final;
CREATE MATERIALIZED VIEW mv_transactions_to_final
TO transactions
AS
SELECT
  parseDateTimeBestEffort(transaction_time) AS transaction_time,
  merch,
  cat_id,
  amount,
  name_1,
  name_2,
  gender,
  us_state,
  lat,
  lon,
  merchant_lat,
  merchant_lon,
  target
FROM transactions_kafka;
