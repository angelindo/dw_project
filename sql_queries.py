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
    artist VARCHAR(500) NULL,
    auth VARCHAR(15) NULL,
    firstName VARCHAR(50) NULL,
    gender CHAR(1) NULL,
    itemInSession INTEGER NULL,
    lastName VARCHAR(100) NULL,
    length REAL NULL,
    level CHAR(4) NULL,
    location VARCHAR(100) NULL,
    method CHAR(3) NULL,
    page VARCHAR(50) NULL,
    registration REAL NULL,
    sessionId INTEGER NULL,
    song VARCHAR(500) NULL,
    status SMALLINT NULL,
    ts TIMESTAMP NULL,
    userAgent VARCHAR(200) NULL,
    userId INTEGER NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_song
(
    num_songs          INTEGER NULL,
    artist_id          VARCHAR(18) NULL,
    artist_latitude    REAL NULL,
    artist_longitude   REAL NULL,
    artist_location    VARCHAR(500) NULL,
    artist_name        VARCHAR(500) NULL,
    song_id            VARCHAR(18) NULL,
    title              VARCHAR(500) NULL,
    duration           REAL NULL,
    year               INTEGER NULL
);
""")

songplay_table_create = ("""
CREATE TABLE fct_songplay
(
    songplay_id INTEGER IDENTITY(0,1) sortkey,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER distkey,
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
    title VARCHAR(500) NOT NULL, 
    artist_id VARCHAR(18) NOT NULL, 
    year INTEGER NULL,
    duration REAL NULL
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE dim_artist
(
    artist_id VARCHAR(18) NOT NULL sortkey, 
    name VARCHAR(500) NULL, 
    location VARCHAR(500) NULL, 
    lattitude REAL NULL, 
    longitude REAL NULL
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
COPY staging_event 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON {}
TIMEFORMAT 'epochmillisecs';
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_song 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fct_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select 
a.ts, 
a.userId, 
a.level,
b.song_id,
b.artist_id,
a.sessionid,
a.location,
a.userAgent
from staging_event a
join staging_song b
on a.song = b.title
where page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
select distinct userId, firstName, lastName, gender, level
from staging_event e1
WHERE page = 'NextSong'
AND userId is not NULL
AND ts = (SELECT MAX(ts) FROM staging_event e2 WHERE e2.userId = e1.userId and e2.page = 'NextSong');
""")

song_table_insert = ("""
INSERT INTO dim_song
select distinct song_id, title, artist_id, year, duration
from staging_song;
""")

artist_table_insert = ("""
INSERT INTO dim_artist
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_song;
""")

time_table_insert = ("""
INSERT INTO dim_time
select ts, EXTRACT(hrs from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts) from staging_event
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
