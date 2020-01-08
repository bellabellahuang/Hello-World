### Project Description

The purpose of this project is to create and implement ETL pipeline for Sparkify to set up data warehouse in AWS Redshift.
    
### Data Pipeline

As Sparkify has their raw data stored in S3, it is easy to load those data into staging tables in Redshift using the COPY queries.  

After the data is successfully imported into the staging tables, it will get transformed and insert into the fact and dimension tables for analysis purpose.
    
### Warehouse Struture
Based on the data examples for events and songs, there are five tables to be set up for analysis.

- **songplays**: used to store records in event data associated with song plays  
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
- **users**: used to store users data in the app  
    user_id, first_name, last_name, gender, level
    
- **songs**: used to store songs in music database  
    song_id, title, artist_id, year, duration
    
- **artists**: used to store artists in music database  
    artist_id, name, location, lattitude, longitude
    
- **time**: used to store timestamps of records in songplays broken down into specific units  
    start_time, hour, day, week, month, year, weekday
        
### How to Use

- Launch a redshift cluster and create an IAM role that has read access to S3.
- Put the credentials in the dwh.cfg file.
- Create tables in redshift by running `python create_tables.py`.
- Load and transform data into warehouse by running `python etl.py`.
    
### Example
After all the steps are completed, it is able to do analysis on the data in the warehouse.
For example, run the following query to get the users that listen to a song named 'All Hands Against His Own'.
    
    SELECT *
    FROM users
    WHERE user_id IN (
      SELECT user_id FROM songplays
      WHERE song_id IN (
          SELECT song_id
          FROM songs
          WHERE title = 'All Hands Against His Own'
      )
    );

