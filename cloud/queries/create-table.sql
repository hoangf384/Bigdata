CREATE SCHEMA bigdata;

CREATE OR REPLACE EXTERNAL TABLE bigdata.log_content
OPTIONS (
  format = 'JSON',
  uris = ['gs://bigdata-proj/raw/log_content/*']
);

CREATE OR REPLACE EXTERNAL TABLE bigdata.log_search
OPTIONS (
   format = 'Parquet',
   uris = ['gs://bigdata-proj/raw/log_search/*.parquet']
);
