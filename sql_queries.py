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
    artist VARCHAR(500),
    auth VARCHAR(15),
    firstName VARCHAR(50),
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(100),
    length FLOAT,
    level CHAR(4),
    location VARCHAR(100),
    method CHAR(3),
    page VARCHAR(50),
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR(500),
    status SMALLINT,
    ts TIMESTAMP,
    userAgent VARCHAR(200),
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_song
(
    num_songs          INTEGER,
    artist_id          VARCHAR(18),
    artist_latitude    FLOAT,
    artist_longitude   FLOAT,
    artist_location    VARCHAR(500),
    artist_name        VARCHAR(500),
    song_id            VARCHAR(18),
    title              VARCHAR(500),
    duration           FLOAT,
    year               INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE fct_songplay
(
    songplay_id INTEGER IDENTITY(0,1) sortkey,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL distkey,
    level CHAR(4) NULL, 
    song_id VARCHAR(18) NOT NULL, 
    artist_id VARCHAR(18) NOT NULL, 
    session_id INTEGER NOT NULL, 
    location VARCHAR(100) NULL, 
    user_agent VARCHAR(200) NULL,
    PRIMARY KEY(songplay_id) 
);
""")

user_table_create = ("""
CREATE TABLE dim_user
(
    user_id INTEGER sortkey distkey, 
    first_name VARCHAR(50) NULL, 
    last_name VARCHAR(100) NULL, 
    gender CHAR(1) NULL, 
    level CHAR(4) NULL,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE dim_song
(
    song_id VARCHAR(18) sortkey, 
    title VARCHAR(500) NOT NULL, 
    artist_id VARCHAR(18) NOT NULL, 
    year INTEGER NULL,
    duration FLOAT NULL,
    PRIMARY KEY (song_id)
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE dim_artist
(
    artist_id VARCHAR(18) sortkey, 
    name VARCHAR(500) NULL, 
    location VARCHAR(500) NULL, 
    lattitude FLOAT NULL, 
    longitude FLOAT NULL,
    PRIMARY KEY (artist_id)
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE dim_time
(
    start_time DATETIME sortkey, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    PRIMARY KEY (start_time)
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
on (a.song = b.title and a.artist = b.artist_name and a.length = b.duration)
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
select distinct start_time, EXTRACT(hrs from start_time), EXTRACT(day from start_time), EXTRACT(week from start_time), EXTRACT(month from start_time), EXTRACT(year from start_time), EXTRACT(weekday from start_time) from fct_songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
