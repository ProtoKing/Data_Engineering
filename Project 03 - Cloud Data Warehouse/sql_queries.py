import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar, 
    first_name varchar,
    gender varchar, 
    item_in_session integer,
    last_name varchar,
    length decimal,
    level varchar,
    location varchar, 
    method varchar, 
    page varchar,
    registration decimal, 
    session_id integer, 
    song varchar, 
    status integer, 
    ts bigint,
    user_agent varchar, 
    user_id varchar)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id varchar,
    num_songs int, 
    artist_id varchar, 
    artist_latitude decimal,
    artist_longitude decimal, 
    artist_location varchar,
    artist_name varchar,
    title varchar, 
    duration decimal,
    year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int PRIMARY KEY IDENTITY(0,1) SORTKEY DISTKEY,
    start_time timestamp,
    user_id varchar NOT NULL,
    level varchar NOT NULL,
    song_id varchar, 
    artist_id varchar, 
    session_id int,
    location varchar, 
    user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar NOT NULL PRIMARY KEY SORTKEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar , 
    level varchar)
    diststyle all
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY SORTKEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration decimal)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (artist_id varchar NOT NULL PRIMARY KEY SORTKEY, 
    artist_name varchar NOT NULL, 
    artist_location varchar, 
    artist_latitude decimal, 
    artist_longitude decimal)
    diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
    (start_time timestamp NOT NULL PRIMARY KEY SORTKEY, 
    hour int, 
    day int,
    week int, 
    month int, 
    year int, 
    weekday int)
    diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    json {};
""").format(
    config.get('S3','LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
    config.get('CLUSTER', 'REGION')
)

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    json 'auto';
""").format(
    config.get('S3','SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('CLUSTER', 'REGION'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
SELECT  TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
        se.user_id,
        se.level,
        ss.song_id, 
        ss.artist_id, 
        se.session_id, 
        se.location,
        se.user_agent
FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong'
AND se.song = ss.title
AND se.artist = ss.artist_name
AND se.length = ss.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id, 
    first_name,
    last_name, 
    gender,
    level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id, 
    title,
    artist_id, 
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time),
    extract(month from start_time),
    extract(year from start_time),
    extract(weekday from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
