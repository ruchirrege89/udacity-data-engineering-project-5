_This project is a part of [Udacity's Data Engineer Nano Degree](https://eu.udacity.com/course/data-engineer-nanodegree--nd027)._

## Overview

This project includes building a data pipeline for Sparkify using Apache Airflow which automates the running of ETL processes to load data.

In the ETL loading process the the songs and logs data in JSON formats are taken from S3 and loaded into Redshift table designed according to STAR schema. The STAR schema allows Sparkify analysts to run analytics in well structured data.

The Airflow data pipeline also includes a data quality check which ensures that redshift tables are loaded correctly every time the pipeline is scheduled.
 
## Overview of Modules

DAG
* `udac_example_dag.py` contains the tasks and relationships of the DAG.

SubDAG
* `load_dimension_table_dag.py` contains tasks to load dimension tables. The subdag has made the DAG concise and abstract without making it difficult to read and understand. This was not necessary for the project but a good learning opportunity for SubDAGs.

Helpers
* `sql_queries.py` contains the SQL queries used in the ETL process.

Operators

* `stage_redshift.py` contains `StageToRedshiftOperator` which copies JSON data from S3 to staging tables in the Redshift.
* `load_dimension.py` contains `LoadDimensionOperator` which loads dimension tables from data in the staging tables.
* `load_fact.py` contains `LoadFactOperator` which loads fact table from data in the staging tables.
* `data_quality.py` contains `DataQualityOperator`, which runs a data quality check by checking two things 1. If the destination table is empty and 2. If the destination tables have more than 1 record.

## Airflow Configuration

* Airflow Connections:
    * AWS credentials - to let airflow pipeline talk to S3
    * Redshift - to load data in Redshift tables
