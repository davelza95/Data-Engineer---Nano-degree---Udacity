# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id SERIAL,
     start_time  TIMESTAMP NOT NULL,
     user_id     INT NOT NULL,
     level       VARCHAR,
     song_id     VARCHAR,
     artist_id   VARCHAR,
     session_id  INT,
     location    VARCHAR,
     user_agent  VARCHAR,
     PRIMARY KEY(songplay_id)
  )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
  (
     user_id    INT,
     first_name VARCHAR,
     last_name  VARCHAR,
     gender     VARCHAR,
     level      VARCHAR,
     PRIMARY KEY(user_id)
  )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
  (
     song_id   VARCHAR PRIMARY KEY,
     title     VARCHAR NOT NULL,
     artist_id VARCHAR NOT NULL,
     year      INT,
     duration  NUMERIC NOT NULL
  ) 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
             (
                          artist_id VARCHAR PRIMARY KEY,
                          name      VARCHAR NOT NULL,
                          location  VARCHAR,
                          latitude DOUBLE PRECISION,
                          longitude DOUBLE PRECISION
             )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
  (
     start_time TIMESTAMP UNIQUE,
     hour       INT,
     day        INT,
     month      INT,
     year       INT,
     weekday    INT
  ) 
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
            (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        songplay_id
            )
            do nothing
""")

user_table_insert = ("""
INSERT INTO users VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        user_id
            )
            do
UPDATE
SET    level = excluded.level
""")

song_table_insert = ("""
INSERT INTO songs VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        song_id
            )
            do nothing
""")

artist_table_insert = ("""
INSERT INTO artists VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        artist_id
            )
            do nothing
""")


time_table_insert = ("""
INSERT INTO time VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        start_time
            )
            do nothing
""")

# FIND SONGS

song_select = ("""
SELECT song_id,
       t1.artist_id
FROM   songs t1
       join artists t2
         ON t1.artist_id = t2.artist_id
WHERE  t1.title = ( %s )
       AND t2.name = ( %s )
       AND t1.duration = ( %s ) 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]