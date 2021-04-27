## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Project Description

As a data engineer, I built an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs, users are listening to. 


### Project Datasets
The datasets that present in S3
**Song data**: s3://udacity-dend/song_data
**Log data**: s3://udacity-dend/log_data

### Files
**create_tables.py** - This file drops and creates table in redshift.

**etl.py** - This file loads data from Amazon S3 into Stage Tables and Inserts Data into Fact and Dimentions

**sql_queries.py** - This file Contains DDL(Data Definition Language) and DML(Data Manipulation Language) for Stage, Fact and Dimention tables.

**dwh.cfg** - This Config file has the connection details for AWS Redshift.


### Schema for Song Play Analysis
Using the song and log datasets, star schema is to be created for performing optimized queries on song play analysis. This includes the following tables.

#### Fact Table
**songplays** - records in log data associated with song plays 
*Columns*: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables
**users** - users in the app
*Columns*: user_id, first_name, last_name, gender, level

**songs** - songs in music database
*Columns*: song_id, title, artist_id, year, duration

**artists** - artists in music database
*Columns*: artist_id, name, location, latitude, longitude

**time** - timestamps of records in songplays broken down into specific units
*Columns*: start_time, hour, day, week, month, year, weekday
