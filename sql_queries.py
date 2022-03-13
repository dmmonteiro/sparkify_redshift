import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# data from S3 buckets - used in copy commands
AWS_KEY = config.get('AWS', 'key')
AWS_SECRET = config.get('AWS', 'secret')
EVENTS_DATA = config.get("S3", "log_data")
SONGS_DATA = config.get("S3", "song_data")
LOG_JSON_PATH = config.get('S3', 'log_jsonpath')

REGION = 'us-west-2'

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

# STAGING EVENTS
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_events(
  artist VARCHAR,
  auth VARCHAR,
  first_name VARCHAR,
  gender VARCHAR,
  item_in_session INT,
  last_name VARCHAR,
  length FLOAT,
  level VARCHAR,
  location VARCHAR,
  method VARCHAR,
  page VARCHAR,
  registration INT8,
  session_id INT,
  song VARCHAR,
  status INT,
  ts INT8,
  user_agent VARCHAR,
  user_id INT
);
""")

# STAGING SONGS
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS public.staging_songs(
  artist_id VARCHAR,
  artist_name VARCHAR,
  artist_latitude DOUBLE PRECISION,
  artist_location VARCHAR,
  artist_longitude DOUBLE PRECISION,
  duration FLOAT,
  num_songs INT,
  song_id VARCHAR,
  title VARCHAR,
  year INT
);
""")

# FACT TABLE - songplays
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS public.songplays(
  songplay_id VARCHAR PRIMARY KEY,
  artist_id VARCHAR,
  level VARCHAR,
  location VARCHAR,
  session_id INT,
  song_id VARCHAR,
  start_time TIMESTAMP NOT NULL,
  user_agent VARCHAR,
  user_id INT NOT NULL
);
""")

# DIMENSION TABLES
# users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS public.users(
  user_id INT PRIMARY KEY,
  first_name VARCHAR,
  last_name VARCHAR,
  gender VARCHAR,
  level VARCHAR
);
""")

# songs
song_table_create = ("""
CREATE TABLE IF NOT EXISTS public.songs(
  song_id VARCHAR PRIMARY KEY,
  artist_id VARCHAR,
  duration FLOAT,
  title VARCHAR,
  year INT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS public.artists(
  artist_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  latitude DOUBLE PRECISION,
  location VARCHAR,
  longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS public.time(
  start_time TIMESTAMP PRIMARY KEY,
  day INT,
  hour INT,
  month VARCHAR,
  week INT,
  week_day VARCHAR,
  year INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY public.staging_events
FROM {}
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
TIMEFORMAT AS 'epochmillisecs'
REGION '{}'
JSON {}
""").format(EVENTS_DATA, AWS_KEY, AWS_SECRET, REGION, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY public.staging_songs
FROM {}
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
TIMEFORMAT AS 'epochmillisecs'
REGION '{}'
JSON '{}'
""").format(SONGS_DATA, AWS_KEY, AWS_SECRET, REGION, 'auto')

# FINAL TABLES

# songplays
songplay_table_insert = ("""
INSERT INTO public.songplays(
  SELECT md5(e.session_id || e.start_time) songplay_id,
         sts.artist_id,
         e.level,
         e.location,
         e.session_id,
         sts.song_id,
         e.start_time,
         e.user_agent,
         e.user_id
  FROM (
      SELECT TIMESTAMP 'epoch' + ste.ts/1000 * INTERVAL '1 second' start_time,
             ste.*
        FROM public.staging_events ste
        WHERE page = 'NextSong'
  ) e
  LEFT JOIN public.staging_songs sts
    ON e.song = sts.title
    AND e.artist = sts.artist_name
    AND e.length = sts.duration
);
""")

# users
user_table_insert = ("""
INSERT INTO public.users(
  SELECT DISTINCT user_id, first_name, last_name, gender, level
  FROM public.staging_events
  WHERE page = 'NextSong'
);
""")

# songs
song_table_insert = ("""
INSERT INTO public.songs(
  SELECT DISTINCT song_id, artist_id, duration, title, year
  FROM public.staging_songs
);
""")

# artists
artist_table_insert = ("""
INSERT INTO public.artists(
  SELECT DISTINCT artist_id,
                  artist_name,
                  artist_latitude,
                  artist_location,
                  artist_longitude
  FROM public.staging_songs
);
""")

# time
time_table_insert = ("""
INSERT INTO public.time(
  SELECT start_time,
         EXTRACT(day FROM start_time),
         EXTRACT(hour FROM start_time),
         EXTRACT(month FROM start_time),
         EXTRACT(week FROM start_time),
         EXTRACT(dayofweek FROM start_time),
         EXTRACT(year FROM start_time)
  FROM public.songplays
);
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
