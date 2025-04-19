-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Improve data quality validation upstream (e.g., warn if position has NULL ReId or invalid KdNr)

-- COMMAND ----------

CREATE OR REPLACE TABLE stg_positions AS
SELECT
  *,
  case when id<0 or id=0 or id="NULL" or id is null then false else true end as id_constraint_flag,
  case when ReID<0 or ReID=0 or ReID="NULL" or ReID is null then false else true end as ReId_constraint_flag,
  case when KdNr<0 or KdNr=0 or KdNr="NULL" or KdNr is null then false else true end as KdNr_constraint_flag,
  case when Nettobetrag <0 or Nettobetrag=0 or Nettobetrag="NULL" or Nettobetrag is null then false else true end as Nettobetrag_constraint_flag, 
  case when Bildnummer<0 or Bildnummer=0 or Bildnummer="NULL" or Bildnummer is null then false else true end as Bildnummer_constraint_flag
FROM positions;

--Sending the valid data (with all columns matching the criteria) to the transformation phase
CREATE OR REPLACE TABLE stg_positions_valid 
  AS SELECT * FROM stg_positions
  WHERE id_constraint_flag is true AND ReId_constraint_flag is true AND KdNr_constraint_flag is true AND Nettobetrag_constraint_flag is true;

--Creating a metadeta table to fecth and asses the unhealthy rows (regardless of even one of the values not matching the criteria)
CREATE OR REPLACE TABLE stg_positions_invalid
  AS SELECT * FROM stg_positions
  WHERE NOT (id_constraint_flag is true OR ReId_constraint_flag is true OR KdNr_constraint_flag is true OR Nettobetrag_constraint_flag is true);


-- COMMAND ----------

CREATE OR REPLACE TABLE stg_invoices AS
SELECT
  *,
  case when ReNummer<0 or ReNummer=0 or ReNummer="NULL" or ReNummer is null then false else true end as ReNummer_constraint_flag,
  case when KdNr<0 or KdNr=0 or KdNr="NULL" or KdNr is null then false else true end as KdNr_constraint_flag,
  case when SummeNetto <0 or SummeNetto=0 or SummeNetto="NULL" or SummeNetto is null then false else true end as SummeNetto_constraint_flag
FROM invoices;

--Sending the valid data (with all columns matching the criteria) to the transformation phase
CREATE OR REPLACE TABLE stg_invoices_valid 
  AS SELECT * FROM stg_invoices
  WHERE ReNummer_constraint_flag is true AND KdNr_constraint_flag is true AND SummeNetto_constraint_flag is true;

--Creating a metadeta table to fecth and asses the unhealthy rows (regardless of even one of the values not matching the criteria)
CREATE OR REPLACE TABLE stg_invoices_invalid
  AS SELECT * FROM stg_invoices
  WHERE NOT (ReNummer_constraint_flag is true OR KdNr_constraint_flag is true OR SummeNetto_constraint_flag is true);


-- COMMAND ----------

CREATE OR REPLACE TABLE stg_customers AS
SELECT
  *,
  case when KdNr not in (select kdNr from invoices) and kdNr not in (select kdNr from invoices) then false else true end as fk_flag,
  case when Verlagsname!=trim(Verlagsname) then trim(Verlagsname) else Verlagsname end as verlagsname_whitespaces,
  case when Region!=trim(Region) then trim(Region) else Verlagsname end as Region_whitespaces
FROM customers;

--Sending the valid data (with all columns matching the criteria) to the transformation phase
CREATE OR REPLACE TABLE stg_customers_valid 
  AS SELECT * FROM stg_customers
  WHERE fk_flag is true AND verlagsname_whitespaces is true AND Region_whitespaces is true;

--Creating a metadeta table to fecth and asses the unhealthy rows (regardless of even one of the values not matching the criteria)
CREATE OR REPLACE TABLE stg_invoices_invalid
  AS SELECT * FROM stg_customers
  WHERE NOT (fk_flag is true OR verlagsname_whitespaces is true OR Region_whitespaces is true);
