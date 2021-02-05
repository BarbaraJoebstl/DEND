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
- make sure you store those credentials in dwh.cfg /don't forget gitignore

### DATA
Have a look at the provided data.


## Create Table Schemas

- `sql_queries.py` holds the `CREATE` statements for each of the tables shown above.
- `create_tables.py` has `DROP` statementes implemented, so that we can reset the database and test our ETL pipeline.

## Build ETL Pipeline
- load data to staging tables on Redshift
- load staging data to analytics data on Redshift