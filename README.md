# Imago_Omama
Data engineer associate test

## The frameworks and platforms used:
- SQL.
- Databricks (Community Version). 

## Assumptions:  
 - During the staging part, certain validation checks, e.g the zahlungsdatum have not been catered because they have been covered under the second part of the second question, in the transformation layer.  
 - Some validation checks have were repetitive amongst all 3 tables, hence, have been used only once (fk constraint).  
 - For Q1, part 2, it is assumed that the revenue is calculated based on the sum of nettobetrag and not the burrotubetrag.  
 - For the Q2, it was assumed that the whole meta data for the failing validity constraints must be fetched.

## How this change would impact downstream reporting
- improved visual transparency in the dashboards
- correct analysis since no missing values casuing ambiguity
- Accurate data since no overlooking nulls. 

## Constraints: 
- There was no access to MSSQL, hence, it was not possible to use TSQL. Instead, manual triggering performance has been queried which sustains no as such warning or alert, but the solution provided could be formed via a trigger as well.
- Since the platform being used was the community version, there was no access to the delta live tables mixed with expect constraint to fetch the validity constraints, hence just the normal delta lake and csv tables have been used.  

## Q3: Modern Tooling (Optional)

### How you'd migrate parts of this pipeline into a modern stack (e.g., from SSIS to dbt or Airflow)
- I would start by initializing airflow and run the SSIS migration scripts to SQL server as external scripts in airflow bash. The staging area would the mssql while the transformations and loading would be done via the cloud dbt, which in turn would be trigerred by the dag initially.

### What parts of the current flow should stay as is for now and why
- Parts: staging part, end user interface, data quality validation constraints, and the sources of the data.
- Why? Since it's a data migration project, majority of the things should stay constant in order to minimize the risk; morever, contemplating on the  UI of the end users can cause inter department disperencies as well. Adding to it, the less the things are interwined, the more the cause of any failure could be recognized (if any).
In order to test the envioronemts, the pipeline should be replicated as it is. Once the pipeline has been tested against the new environment, new checks and rules can be added upon. 

### Any risks in changing how invoice/position data is processed
- Loss of data due to schema shifts.
- Delay in data migration.
- Loss of historical data (hence, a strong backup should always be thought upon).

* I have attached the files in the repository; however, just to be more sure, I am attaching below the link to the code: 
https://community.cloud.databricks.com/browse/folders/1559708265193692?o=1032570082255084

*Google Drive:
https://drive.google.com/drive/folders/12G2cqQHLJexZjQ1zaF7ka5OiKhdVtxxK?usp=sharing 

