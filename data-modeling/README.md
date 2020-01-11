### Project Summary

This database schema is designed to help the analytics team from Sparkify to query on the song play data in an efficient way. They would like to figure out that what songs users like to listen to.

### Database Design

According to the analytical goal and the provided dataset, a database schema is designed and there are five tables for data storage. **songplays** is the fact table that contains all the data for user activities. Other tables, including **users**, **songs**, **artists** and **time**, stores details of each category.

- **songplays**

    Songplay records including songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location and user_agent.
    
- **users**

    Users data in the app including user_id, first_name, last_name, gender and level.
    
- **songs**

    Songs in music database including song_id, title, artist_id, year and duration.
    
- **artists**

    Artists for the songs including artist_id, name, location, latitude and longitude
    
- **time**

    Timestamps of songplay records broken down into hour, day, week, month, year and weekday
    
### Scripts Files

Here is an explanation of the usage of each script.

- **sql_queries.py**

    It contains all the queries for database setup and data ingest.
    
- **create_tables.py**

    It is used to create tables in the database.
    
- **etl.py**

    It is used for data conversion and data import. There are two pipeline in the script. One for song data import and the other one for log data import.
    
### How to Setup

Usage of this project is very simple. Prepare the data for import in the data directory. Data for songs should be collected under the song_data directory and data for logs should be collected under the log_data directory.

- **table creation**

    Run the `create_table.py` to create tables in database.
    
- **data ingest**

    Run the `etl.py` to import songs and logs data into database.

