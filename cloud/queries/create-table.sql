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

CREATE OR REPLACE EXTERNAL TABLE `bigdata.raw_mapping`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://bigdata-proj/raw/mapping/mapping.json']
);
