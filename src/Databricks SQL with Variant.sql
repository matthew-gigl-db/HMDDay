-- Databricks notebook source
DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE schema_use STRING DEFAULT 'hm_dday';

-- COMMAND ----------

SET VARIABLE catalog_use = :`catalog_use`; 
SET VARIABLE schema_use = :`schema_use`; 

-- COMMAND ----------

SELECT 
  catalog_use
  ,schema_use
;

-- COMMAND ----------

USE IDENTIFIER(catalog_use || '.' || schema_use);

-- COMMAND ----------

SELECT
  current_catalog()
  ,current_schema()
;

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

CREATE TABLE my_table AS
SELECT * FROM read_files('/Volumes/main/hm_dday/landing/', format => 'text');

-- COMMAND ----------

SELECT * from my_table;

-- COMMAND ----------

LIST '/Volumes/' || catalog_use || schema_use || '/landing');

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY FUNCTION get_storage_location(volume_name STRING, host STRING, pat STRING) 
RETURNS STRING
LANGUAGE PYTHON
AS $$
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  return w.volumes.read(name=volume_name).storage_location
$$;

-- COMMAND ----------

SELECT get_storage_location('main.hm_dday.landing') AS storage_location;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE storage_location STRING;

SET VAR storage_location = (DESCRIBE VOLUME landing).storage_location;

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION 

-- COMMAND ----------

COPY INTO '/Volumes/' || catalog_use || '/' || schema_use || '/landing/raw_json_files'
FROM 's3://hls-eng-data-public/data/synthea/fhir/fhir/*json'
FILEFORMAT = JSON;

-- COMMAND ----------

CREATE TABLE 
