-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Loading the data

-- COMMAND ----------

CREATE TABLE dwh_positions (
  position_id BIGINT GENERATED ALWAYS AS IDENTITY,
  id INT,
  ReId INT,
  KdNr STRING,
  Nettobetrag STRING,
  Bildnummer STRING,
  VerDatum STRING)
using delta

-- COMMAND ----------

CREATE TABLE dwh_customers (
  custmerID BIGINT GENERATED ALWAYS AS IDENTITY,
  id INT,
  Kdnr INT,
  Verlagsname STRING,
  Region STRING) using DELTA

-- COMMAND ----------

CREATE TABLE dwh_invoices (
  positions_id BIGINT GENERATED ALWAYS AS IDENTITY,
  ReNummer INT,
  SummeNetto DOUBLE,
  MwStSatz INT,
  ZahlungsbetragBrutto STRING,
  KdNr INT,
  Summenebenkosten STRING,
  ReDatum TIMESTAMP,
  Zahlungsdatum STRING)
USING delta