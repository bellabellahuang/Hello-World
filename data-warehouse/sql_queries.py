import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXITS staging_evnets (
        artist          VARCHAR,
        auth            VARCHAR,
        first_name      VARCHAR,
        gender          CHAR(1),
        item_in_session INTEGER,
        last_name       VARCHAR,
        length          DECIMAL,
        level           CHAR(10),
        location        VARCHAR,
        method          CHAR(10),
        page            CHAR(10),
        registration    DECIMAL,
        session_id      INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              INTEGER,
        user_agent      VARCHAR,
        user_id         INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXITS staging_songs (
        num_songs        INTEGER,
        artist_id        VARCHAR,
        artist_latitude  DECIMAL,
        artist_longitude DECIMAL,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         DECIMAL,
        year             INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  INTEGER IDENTITY(0, 1),
        start_time   BIGINT NOT NULL,
        user_id      INTEGER NOT NULL,
        level        VARCHAR NOT NULL,
        song_id      VARCHAR,
        artist_id    VARCHAR,
        session_id   INTEGER,
        location     VARCHAR,
        user_agent   VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name  VARCHAR NOT NULL,
        gender     CHAR(1),
        level      VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id    VARCHAR PRIMARY KEY,
        title      VARCHAR NOT NULL,
        artist_id  VARCHAR NOT NULL,
        year       INTEGER,
        duration   DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id  VARCHAR PRIMARY KEY,
        name       VARCHAR NOT NULL,
        location   VARCHAR,
        lattitude  DECIMAL,
        longitude  DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  BITINT PRIMARY KEY,
        hour        INTEGER NOT NULL,
        day         INTEGER NOT NULL,
        week        INTEGER NOT NULL,
        month       INTEGER NOT NULL,
        year        INTEGER NOT NULL,
        weekday     INTEGER NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {S3_LOG_DATA}
    IAM_ROLE {IAM_ROLE}
    REGION 'us-west-2';
""").format(S3_LOG_DATA=config['S3']['LOG_DATA'], IAM_ROLE=config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {S3_SONG_DATA}
    IAM_ROLE {IAM_ROLE}
    REGION 'us-west-2';
""").format(S3_SONG_DATA=config['S3']['SONG_DATA'], IAM_ROLE=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        events.ts AS start_time,
        events.user_id,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.session_id,
        events.location,
        evnets.user_agent
    FROM staging_events AS events JOIN staging_songs AS songs
    ON events.song = songs.title
    WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT  DISTINCT ts,
            EXTRACT(hour FROM dt) AS hour,
            EXTRACT(day FROM dt) AS day,
            EXTRACT(week FROM dt) AS week,
            EXTRACT(month FROM dt) AS month,
            EXTRACT(year FROM dt) AS year,
            EXTRACT(weekday FROM dt) AS weekday
    FROM(
        SELECT DISTINCT ts, '1970-01-01'::date + ts/1000 * interval '1 second' AS dt
        FROM staging_events
    );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

