-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC %md
-- MAGIC ##Capture missing or delayed payments more gracefully
-- MAGIC

-- COMMAND ----------

--Creating a seperate payments table for missing/delayed payments
create or replace view zahlungsdatum_flag as (
  select *,
  case when zahlungsdatum = "NULL" or zahlungsdatum is null then false else true end as zahlungsdatum_flag
  from invoices);

create or replace table missing_or_delayed_payments using delta
select ReNummer, KdNr, ZahlungsbetragBrutto, Zahlungsdatum
from zahlungsdatum_flag
where zahlungsdatum_flag is false;

select * from missing_or_delayed_payments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flag or isolate “placeholder” media positions for separate handling

-- COMMAND ----------

--Isolating the rows with the "placeholder" media
CREATE OR REPLACE VIEW v_flag_bild as (
  select *,
  case when Bildnummer = '100000000' then "placeholder" else "normal" end as position_type_flag
  FROM positions
);

CREATE TABLE invalid_bild
select *
from v_flag_bild
where position_type_flag = "placeholder"

-- COMMAND ----------

