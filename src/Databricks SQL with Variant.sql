-- Databricks notebook source
SELECT REPLACE(SPLIT(current_user(), '@')[0], '.', '-');

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE schema_use STRING DEFAULT 'hm_dday';
DECLARE OR REPLACE VARIABLE host_url STRING DEFAULT 'https://adb-984752964297111.11.azuredatabricks.net/';
DECLARE OR REPLACE VARIABLE secret_scope STRING DEFAULT (SELECT REPLACE(SPLIT(current_user(), '@')[0], '.', '-'));
DECLARE OR REPLACE VARIABLE token STRING DEFAULT (SELECT secret(secret_scope, 'databricks_pat'));

-- COMMAND ----------

SET VARIABLE catalog_use = :`catalog_use`;
SET VARIABLE schema_use = :`schema_use`; 

-- COMMAND ----------

SELECT 
  catalog_use
  ,schema_use
;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(catalog_use || '.' || schema_use);

-- COMMAND ----------

USE IDENTIFIER(catalog_use || '.' || schema_use);

-- COMMAND ----------

SELECT
  current_catalog()
  ,current_schema()
;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS IDENTIFIER(catalog_use || '.' || schema_use || '.landing');

-- COMMAND ----------

SHOW VOLUMES;

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
