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

DECLARE OR REPLACE VARIABLE list_stmnt STRING;

SET VAR list_stmnt = "LIST '/Volumes/" || catalog_use || "/" || schema_use || "/landing/'";

SELECT list_stmnt;

-- COMMAND ----------

EXECUTE IMMEDIATE list_stmnt;

-- COMMAND ----------

SET VAR list_stmnt = "LIST '/Volumes/" || catalog_use || "/" || schema_use || "/landing/fhir/'";

SELECT list_stmnt;

EXECUTE IMMEDIATE list_stmnt;

-- COMMAND ----------

DROP TABLE IF EXISTS fhir_bronze;

CREATE OR REFRESH STREAMING TABLE fhir_bronze 
COMMENT 'Ingest FHIR JSON records as Full Text STRING'
TBLPROPERTIES (
  'quality' = 'bronze'
)
AS SELECT
  _metadata as file_metadata
  ,* 
FROM STREAM read_files(
  '/Volumes/main/hm_dday/landing/fhir/'
  ,format => 'text'
  ,wholeText => true
)

-- COMMAND ----------

SELECT * FROM fhir_bronze;

-- COMMAND ----------

DROP TABLE IF EXISTS fhir_bronze_variant;

CREATE OR REFRESH STREAMING TABLE fhir_bronze_variant 
COMMENT 'Evaluate FHIR JSON records as VARIANT'
TBLPROPERTIES (
  'quality' = 'bronze'
  ,'pipelines.channel' = 'PREVIEW'
  ,'delta.feature.variantType-preview' = 'supported'
)
AS SELECT
  file_metadata
  ,try_parse_json(value) as fhir 
FROM STREAM fhir_bronze;

-- COMMAND ----------

select * from fhir_bronze_variant;

-- COMMAND ----------

SELECT
  file_metadata
  ,fhir
  ,entry.*
FROM 
  fhir_bronze_variant
  ,LATERAL variant_explode(fhir:entry) as entry

-- COMMAND ----------

SELECT
  file_metadata
  ,CAST(entry.value:fullUrl AS STRING) as fullUrl
  ,CAST(entry.value:resource.resourceType AS STRING) as resourceType
  ,entry.value as entry
FROM 
  fhir_bronze_variant
  ,LATERAL variant_explode(fhir:entry) as entry

-- COMMAND ----------

SELECT
  file_metadata
  ,CAST(entry.value:fullUrl AS STRING) as fullUrl
  ,CAST(entry.value:resource.resourceType AS STRING) as resourceType
  ,resource.*
FROM 
  fhir_bronze_variant
  ,LATERAL variant_explode(fhir:entry) as entry
  ,LATERAL variant_explode(entry.value:resource) as resource

-- COMMAND ----------

DROP TABLE IF EXISTS fhir_resources;

CREATE OR REFRESH STREAMING TABLE fhir_resources 
COMMENT 'Exploded FHIR Resources'
TBLPROPERTIES (
  'quality' = 'bronze'
  ,'pipelines.channel' = 'PREVIEW'
  ,'delta.feature.variantType-preview' = 'supported'
)
AS SELECT
  file_metadata
  ,CAST(entry.value:fullUrl AS STRING) as fullUrl
  ,CAST(entry.value:resource.resourceType AS STRING) as resourceType
  ,resource.*
FROM 
  STREAM(fhir_bronze_variant)
  ,LATERAL variant_explode(fhir:entry) as entry
  ,LATERAL variant_explode(entry.value:resource) as resource

-- COMMAND ----------

SELECT 
  resourceType
  ,count(distinct fullUrl) as cnt
FROM 
  main.hm_dday.fhir_resources
GROUP BY ALL
ORDER BY cnt DESC;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE patient_keys ARRAY<STRING>;

SET VAR patient_keys = (
  SELECT 
    collect_list(DISTINCT key)
  FROM 
    fhir_resources 
  WHERE 
    resourceType = 'Patient'
);

select patient_keys;

-- COMMAND ----------

SELECT
  *
FROM (
  SELECT
    file_metadata
    ,fullUrl
    ,key
    ,value
  FROM 
    fhir_resources
  WHERE 
    resourceType = 'Patient')
  PIVOT (
    first(value) FOR key IN ("multipleBirthInteger","name","birthDate","id","address","gender","telecom","resourceType","text","communication","maritalStatus","identifier","multipleBirthBoolean","deceasedDateTime","meta","extension")
  )

-- COMMAND ----------

CREATE OR REPLACE TABLE patient 
AS SELECT
  *
FROM (
  SELECT
    file_metadata
    ,fullUrl
    ,key
    ,value
  FROM 
    fhir_resources
  WHERE 
    resourceType = 'Patient')
  PIVOT (
    first(value) FOR key IN ("multipleBirthInteger","name","birthDate","id","address","gender","telecom","resourceType","text","communication","maritalStatus","identifier","multipleBirthBoolean","deceasedDateTime","meta","extension")
  );

-- COMMAND ----------

SHOW CREATE TABLE patient;

-- COMMAND ----------

WITH patient_fixed as (
  SELECT
    CAST(gender as STRING) as gender
    ,CAST(id as STRING) as patient_id
    ,CAST(birthDate as DATE) as birthDate
  FROM main.hm_dday.patient
)
SELECT 
  gender
  ,count(distinct patient_id) as cnt
  ,AVG(DATEDIFF(current_date(), birthDate) / 365.25) as avg_age
FROM patient_fixed
GROUP BY gender

-- COMMAND ----------

WITH patient_fixed as (
  SELECT
    CAST(id as STRING) as patient_id
    ,CAST(address:[0].city as STRING) as primary_city
  FROM main.hm_dday.patient
)
SELECT 
  primary_city
  ,count(distinct patient_id) as cnt
FROM patient_fixed
GROUP BY primary_city
ORDER BY cnt DESC

-- COMMAND ----------

WITH address_fixed as (
  SELECT
    CAST(id as STRING) as patient_id
    -- ,address.value as address
    -- ,extension.value as extension
    ,CAST(coordinates.value:url as STRING) as coordinate_type
    ,CAST(coordinates.value:valueDecimal as FLOAT) as coordinates
  FROM hm_dday.patient
  ,LATERAL variant_explode(address) as address
  ,LATERAL variant_explode(address.value:extension) as extension
  ,LATERAL variant_explode(extension.value:extension) as coordinates
)
SELECT
  *
FROM
  address_fixed
  PIVOT (
    first(coordinates) FOR coordinate_type IN ("latitude","longitude")
  )
;
