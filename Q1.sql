-- Databricks notebook source
select * from invoices

-- COMMAND ----------

select * from customers_3_csv

-- COMMAND ----------

select * from positions

-- COMMAND ----------

--Checking for the fk relationship 
SELECT DISTINCT p.ReId
FROM positions p
WHERE p.ReId NOT IN (
    SELECT ReNummer FROM invoices
);

--double checking against the many to many relationship to ensure a safe join
select p.reId, count(p.reId)
from positions p 
group by ReId
having count(p.reId)>1

select i.renummer, count(i.renummer)
from invoices i 
group by renummer
having count(i.ReNummer)

--Querying the total positions having no payment info
select count(p.id)
FROM positions p
INNER JOIN invoices i 
on p.ReId=i.ReNummer
WHERE ZahlungsbetragBrutto = "Null" or ZahlungsbetragBrutto='0.00'


-- COMMAND ----------

select sum(Nettobetrag)
from positions
where Bildnummer='100000000'

-- COMMAND ----------

SELECT COUNT(i.ReNummer)
FROM invoices i
LEFT JOIN positions p ON i.ReNummer = p.ReId
WHERE p.ReId IS NULL;
