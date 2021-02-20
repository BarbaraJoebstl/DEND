# Project 4: Spark and Data Lakes

## About

This is an assignment as the 4th part of the Data Engineer Nanodegree - _Data Lakes_.
The aim of this course was to learn

- Why we nee Data Lakes
- Big data technology effect on data warehousing
- How to do Data Lakes
- Big Data on AWS_The Options
- Data Lake Issues

### Data introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Assignment

The task is to build an ETL pipeline for a data lake hosted on S3. We need to load data from S3, process the data into the analytics tables using Spark, and load them back into S3. This Spark process will be deployed on a cluster using AWS.

## Prerequisites

### Locally

- Set up conda environment with Spark installed

### AWS

### AWS

- AWS CLI installed [Help](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- Set up Access credentials using AWS IAM. Create a User with admin rights and programatic access. Check with `aws iam list-users`
- Create an EMR Cluster
  -- `aws emr create-default-roles` to create an EMR_EC2_DefaultRole for your account
  -- launch a cluter `emr create-cluster --name {your-name} --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName={yourKey} --instance-type m5.xlarge --instance-count 3 `
  Make sure you have the .pem for your EC2 in place. Don't forget to terminate the cluster once you are done or add the `--auto-terminate` flag, when creating the cluster
- Create an S3 Bucket for the output data. Make sure the permissions are set correctly
- Connect to cluster via SSH
  -- Edit the security groups inbound rules to authorize SSH traffic (port 22) from your computer [Help](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-ssh-prereqs.html)
  -- Verify connection to the Master node: EC2 -> Instances -> your master node -> Connect to instance [Help](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)

## Build ETL Pipeline

#### Data

Load the data from S3 with spark.read.json() and have a looke at the schemas
![Screenshot 2021-02-20 at 11 32 51](https://user-images.githubusercontent.com/6280553/108597445-f9452b00-7380-11eb-888f-06513ea5be98.png)
![Screenshot 2021-02-20 at 11 33 03](https://user-images.githubusercontent.com/6280553/108597446-fa765800-7380-11eb-9b73-549106d430a0.png)

#### Table schemas for analysis

Create the desired tables
![Startschema](https://user-images.githubusercontent.com/6280553/107052686-dc77f780-67c5-11eb-9907-a33205460f4a.png)

- Write tables to parquet
  and output it given S3 bucket

- check out `Assignment_Data_Lake.ipynb` to run the code locally with the provided sample data.
- for production the provided `etl.py` can be used.

## Run in on AWS

Once you made sure everythinng works nice in your notebook with the sample data you can run AWS.

## Further reading /Additional Info

### Python

[Docstrings](https://www.python.org/dev/peps/pep-0257/)
[Why PEP8](https://realpython.com/python-pep8/)
[PEP 8 Styleguide](http://pep8online.com/)

### EMR - Elastic Map Reduce

Since a Spark cluster includes multiple machines, in order to use Spark code on each machine, we would need to download and install Spark and its dependencies. This is a manual process. Elastic Map Reduce is a service offered by AWS that negates the need for you, the user, to go through the manual process of installing Spark and its dependencies for each machine.
`emr create-cluster --name {your-name} --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName={yourKey} --instance-type m5.xlarge --instance-count 3 `

Make sure you have the .pem for your EC2 in place.

Make sure to terminate the cluster once you are done or add the `--auto-terminate` flag, when creating the cluster

### Differnces S3 (Simple Storage Service) and HDFS (Hadoop Distributed File System)

AWS S3 is an object storage system that stores the data using key value pairs, namely bucket and key, and HDFS is an actual distributed file system which guarantees fault tolerance. HDFS achieves fault tolerance by having duplicate factors, which means it will duplicate the same files at 3 different nodes across the cluster by default (it can be configured to different numbers of duplication).
HDFS has usually been installed in on-premise systems, and traditionally have had engineers on-site to maintain and troubleshoot Hadoop Ecosystem, which cost more than having data on cloud. Due to the flexibility of location and reduced cost of maintenance, cloud solutions have been more popular. With extensive services you can use within AWS, S3 has been a more popular choice than HDFS.
Since AWS S3 is a binary object store, it can store all kinds of format, even images and videos. HDFS will strictly require a certain file format - the popular choices are avro and parquet, which have relatively high compression rate and which makes it useful to store large dataset.

### Parquet

https://www.upsolver.com/blog/apache-parquet-why-use
