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


- `create_tables.py` has `DROP` statementes implemented, so that we can reset the database and test our ETL pipeline.

### sql_queries.py
- There are no constraints applied to the staging tables, as they are a clear copy of the source.
- All data tpyes are suported by SQL Data Warehouse.
- SQL data types are divided into categories.
- The `CREATE` statements specify all columns with appropriate data types and constraints for each table.


## Build ETL Pipeline
- `INSERT` and `JOIN` statements are done correctly.
- load data to staging tables on Redshift
- load staging data to analytics data on Redshift


## Further reading
### Table Creation
[Data warehouse ](https://github.com/Huachao/azure-content/blob/master/articles/sql-data-warehouse/sql-data-warehouse-develop-table-design.md)
[Data categories](https://www.journaldev.com/16774/sql-data-types)
[Constraints](https://www.nuwavesolutions.com/constraints-and-indexes/)

### ETL
[Different types of joins](https://www.dofactory.com/sql/join)
[When to use joins](https://chartio.com/resources/tutorials/sql-joins-explained/)
[SQL Select distinct](https://www.dofactory.com/sql/select-distinct)
[Select distinct examples](http://www.mysqltutorial.org/mysql-distinct.aspx)

### Python
[Docstrings](https://www.python.org/dev/peps/pep-0257/)
[Why PEP8](https://realpython.com/python-pep8/)
[PEP 8 Styleguide](http://pep8online.com/)