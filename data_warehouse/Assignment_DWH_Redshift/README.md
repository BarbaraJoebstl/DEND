# Project 3: Song Play Analysis with S3 and Redshift

## About

This is an assignment as the 3 Part of the Data Engineer Nanodegree -  *Data Warehouse*.
The aim of this course was to learn the
- Basics of a Data Warehouses
- Basics of cloudcomputing and AWS
- Implementing a Data Warehouses on AWS

### Data introduction 
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Assignment
The task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Prerequisites
### AWS
- Create an `IAM` User with AdminRights 
- Create redshift cluster to connect to
- Create a `dwh.cfg` where you store your credentials
- If you want to do this via infrastructure as code, you can run `Assignment_DWH.ipynb`

### DATA
Load the data from S3 into a dataframe, to see whats going on.
![Screenshot 2021-02-05 at 14 30 53](https://user-images.githubusercontent.com/6280553/107052680-db46ca80-67c5-11eb-93d5-bb849979c2bd.png)
![Screenshot 2021-02-05 at 14 31 40](https://user-images.githubusercontent.com/6280553/107052681-dbdf6100-67c5-11eb-82ba-f8e30ee1c66a.png)

## Table Schemas
![staging_tables](https://user-images.githubusercontent.com/6280553/107052684-dc77f780-67c5-11eb-87c8-c6ae72e4886f.png)
![Startschema](https://user-images.githubusercontent.com/6280553/107052686-dc77f780-67c5-11eb-9907-a33205460f4a.png)

- `sql_queries.py` holds the `CREATE` statements for each of the tables shown above.
- `create_tables.py` has `DROP` statementes implemented, so that we can reset the database and test our ETL pipeline.

## ETL Pipeline
`etl.py`:
- load data to staging tables on Redshift
- load staging data to analytics data on Redshift
