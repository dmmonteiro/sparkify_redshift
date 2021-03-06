# Data Engineering Nanodegree Project: Data Warehousing with Redshift

This solution comprises an ETL pipeline built in Python that extracts and transforms data from an S3 bucket, stage them in Redshift, and transforms data into a set of dimensional tables

## Project Datasets

### Song Dataset

Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.

```javascript
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what the data in a log file, `2018-11-12-events.json`, looks like.

![log dataset preview](/images/log-data.png)

## Database Schema for Song Play Analysis

The star schema was built and optimized for queries on song play analysis. It includes the following tables:

### Fact Table

**songplays**: records in log data associated with song plays i.e. records with page NextSong

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

**users**: users in the app
        
    user_id, first_name, last_name, gender, level
**songs**: songs in music database
    
    song_id, title, artist_id, year, duration
**artists**: artists in music database
    
    artist_id, name, location, latitude, longitude
**time**: timestamps of records in songplays broken down into specific units
    
    start_time, hour, day, week, month, year, weekday


## Steps to reproduce

- The solution was built using Python 3.8.5 and all dependencies are listed in requirements.txt
To install them, you can run the following command:
```
pip install -r requirements.txt
```

- To test the application, you will need to modify `dwh.cfg` and provide AWS credentials (access key and a secret key) and setup a Redshift cluster

- Run the application:
```
python create_tables.py
python etl.py
```

- Once you have created the DW, you can run queries over the tables using the jupyter notebook named `analytics.ipynb`

## ER diagram
![log dataset preview](/images/sparkifydb.png)