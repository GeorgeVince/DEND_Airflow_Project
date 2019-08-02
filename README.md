# DEND - Airflow Project

## Project Description
Apache airflow is used to schedule and manage Data Pipelines. In this project I have demonstrated the ability to schedule multiple ETL steps using Airflow.
Initially data resides in two S3 buckets in JSON format, data is the transferred into two staging tables in redshift before being paritioned into a star schema for analysis.
Custom operators are used to perform the following tasks:
1) Stage events and songs from S3 - > Redshift
2) Fill the FACT table using the events and songs Redshift tables.
3) Fill the DIMENSION tables using the event and songs Redshift tables.
4) Run checks to ensure each of the dimension tables contain data.

## Prerequisites
- Apache airflow - deploy using docker (https://github.com/puckel/docker-airflow)
- AWS Account
- Redshift Cluster 

### Airflow Credentials:

#### AWS Account
- Conn Id: Enter `aws_credentials`.
- Conn Type: Enter `Amazon Web Services`.
- Login: Enter `AWS ID`
- Password: `AWS_SECRET_ACCESS_KEY`

#### Redshift account details
- Conn Id: Enter `redshift`.
- Conn Type: Enter `Postgres`.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end..
- Schema: Enter `dev`. (The database you want to connect to)
- Login: Enter `awsuser`.
- Password: Enter the `password` you created when launching your Redshift cluster.
- Port: Enter `5439`.

## File Descriptions

### dags
- `s3_to_redshift_dag.py` - Contains the DAG for running the pipeline.
- `create_tables.sql` - the SQL required to create tables.

### plugins/helpers
- `sql_queries.py` - SQL queries used to insert data from staging -> fact and dimension tables.

### operators
- `data_quality.py` - operator used to check fact tables contain data.
- `load_dimension.py` - loads data from staging -> dimension.
- `load_fact.py` - loads data from staging -> fact.
- `stage_redshift.py` - stage data from S3 buckets into staging tables in Redshift.