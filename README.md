# Data Warehousing with AWS Redshift

## Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. I built a ETL pipeline that extracts data from S3, processed them using Amazon Elastic Mapreduce and loaded back into S3. This will enable data scientist to perform analyses.

## Datasets
The dataset used for this project are song and log datasets.
### 1. Song dataset
This is a subset of real data from the million song dataset. Each files is on json format and contains metadata about a song and the artist of the song.
### 2. Log dataset
This consist of log files in json format generated by the even simulator based on songs in the data set.


## ETL pipeline
I extracted data form the song and log dataset and inserted processed them using spark and loaded the data into S3 bucket as dimension tables.

## Project File
1. **dl.cfg**- Contains AWS configuration (Access key and secret key.
2. **etl.py**- It executes scripts to extract json file from S3, processes them using spark and also loading it into an S3 bucktet
3. **READme**- Documentatation of the project.

### How to run the project
1. The EMR cluster must be created.
2. Input the AWS access and secret key into "dl.cfg" file.
3. Run etl.py to execute the script (type python etl.py in terminal)
