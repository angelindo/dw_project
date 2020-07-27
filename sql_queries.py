import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song;"
songplay_table_drop = "DROP TABLE IF EXISTS fct_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_event 
(
    num_songs          INTEGER NULL,
    artist_id          VARCHAR(18) NULL,
    artist_latitude    REAL NULL,
    artist_longitude   REAL NULL,
    artist_location    VARCHAR(100) NULL,
    artist_name        VARCHAR(100) NULL,
    song_id            VARCHAR(18) NULL,
    title              VARCHAR(100) NULL,
    duration           REAL NULL,
    year               INTEGER NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_song
(
    artist VARCHAR(100) NULL,
    auth VARCHAR(15) NULL,
    firstName VARCHAR(50) NULL,
    gender CHAR(1) NULL,
    itemInSession INTEGER NULL,
    lastName VARCHAR(100) NULL,
    length REAL NULL,
    level CHAR(4) NULL,
    location VARCHAR(100) NULL,
    method CHAR(3) NULL,
    page VARCHAR(15) NULL,
    registration REAL NULL,
    sessionId INTEGER NULL,
    song VARCHAR(100) NULL,
    status SMALLINT NULL,
    ts BIGINT NULL,
    userAgent VARCHAR(200) NULL,
    userId INTEGER NULL
);
""")

songplay_table_create = ("""
CREATE TABLE fct_songplay
(
    songplay_id INTEGER IDENTITY(0,1) sortkey,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL distkey,
    level CHAR(4), 
    song_id VARCHAR(18), 
    artist_id VARCHAR(18), 
    session_id INTEGER NOT NULL, 
    location VARCHAR(100), 
    user_agent VARCHAR(200)
);
""")

user_table_create = ("""
CREATE TABLE dim_user
(
    user_id INTEGER NOT NULL sortkey distkey, 
    first_name VARCHAR(50) NULL, 
    last_name VARCHAR(100) NULL, 
    gender CHAR(1) NULL, 
    level CHAR(4) NULL
);
""")

song_table_create = ("""
CREATE TABLE dim_song
(
    song_id VARCHAR(18) NOT NULL sortkey, 
    title VARCHAR(100) NOT NULL, 
    artist_id VARCHAR(18) NOT NULL, 
    year INTEGER NULL,
    duration REAL NULL
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE dim_artist
(
    artist_id VARCHAR(18) NOT NULL sortkey, 
    name VARCHAR(100) NULL, 
    location VARCHAR(100) NULL, 
    lattitude VARCHAR(100) NULL, 
    longitude VARCHAR(100) NULL
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE dim_time
(
    start_time DATETIME NOT NULL sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_event FROM {}
CREDENTIALS 'aws_iam_role={}'
compupdate off
STATUPDATE OFF
region 'us-west-2'
json 'auto';
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"))

staging_songs_copy = ("""
COPY staging_song FROM {}
CREDENTIALS 'aws_iam_role={}'
compupdate off
STATUPDATE OFF
region 'us-west-2'
json {};
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
