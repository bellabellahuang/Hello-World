### Project Description

The purpose of this project is to create and implement ETL pipeline for Sparkify to set up data lake in S3 through Spark.
    
### Data Pipeline

As Sparkify has their raw data of songs and logs stored in S3, it is easy to load those data into Spark.  

After data is loaded into Spark and dataframe is successfully created, we can filter and transform raw data to fact and dimension tables according to analysis purpose.

The transformed data will be exported to S3 as our data lake.
    
### Warehouse Struture
Based on the data examples for events and songs, there are five tables to be set up for analysis.

- **songplays**: used to store records in event data associated with song plays  
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month, year
    location example: s3a://udacity-sparkify-datalake/songplays/year=xxxx/month=xx/xxx.parquet
    
- **users**: used to store users data in the app  
    user_id, first_name, last_name, gender, level
    location example: s3a://udacity-sparkify-datalake/users/xxx.parquet
    
- **songs**: used to store songs in music database  
    song_id, title, artist_id, year, duration
    location example: s3a://udacity-sparkify-datalake/songs/year=xxxx/artist_id=xxx/xxx.parquet
    
- **artists**: used to store artists in music database  
    artist_id, name, location, lattitude, longitude
    location example: s3a://udacity-sparkify-datalake/artists/xxx.parquet
    
- **time**: used to store timestamps of records in songplays broken down into specific units  
    start_time, hour, day, week, month, year, weekday
    location example: s3a://udacity-sparkify-datalake/time/year=xxxx/month=xx/xxx.parquet
        
### How to Use

- Place the valid AWS credentials in dl.cfg.
- Create a S3 bucket for data lake storage. For example, "s3a://udacity-sparkify-datalake/". Update the `output_data` value in `etl.py` if needed.
- Process data lake ETL by running `python etl.py`.