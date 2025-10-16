CREATE EXTERNAL TABLE IF NOT EXISTS datapipeline.weather_data (
  source STRING,
  year INT,
  mean DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://data-pipeline-joaoventura/processed/'
TBLPROPERTIES ('skip.header.line.count'='1');