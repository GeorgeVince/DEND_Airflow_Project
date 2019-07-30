class SqlQueries:

    s3_copy = ("""
    COPY {}
    FROM  '{}'s
    access_key_id '{}'
    secret_access_key '{}'
    compupdate off region 'us-west-2'
    JSON {} truncatecolumns;
     """)


    staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS public.staging_events (artist     VARCHAR,
                                                                            auth           VARCHAR,
                                                                            firstName      VARCHAR,
                                                                            gender         VARCHAR,
                                                                            itemInSession  SMALLINT,
                                                                            lastName       VARCHAR,
                                                                            length         FLOAT,
                                                                            level          VARCHAR,
                                                                            location       VARCHAR,
                                                                            method         VARCHAR,
                                                                            page           VARCHAR,
                                                                            registration   FLOAT,
                                                                            sessionId      SMALLINT,
                                                                            song           VARCHAR,
                                                                            status         INT,
                                                                            TS             FLOAT,
                                                                            userAgent      VARCHAR,
                                                                            userId         INT);""")

    staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs        INT,
                                                                             artist_id        VARCHAR,
                                                                             artist_latitude  FLOAT,
                                                                             artist_longitude FLOAT,
                                                                             artist_location  VARCHAR,
                                                                             artist_name      VARCHAR,
                                                                             song_id          VARCHAR,
                                                                             title            VARCHAR,
                                                                             duration         FLOAT,
                                                                             year             INT);""")

    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id   INT     IDENTITY(0,1)  NOT NULL,
                                                                 start_time     FLOAT                  NOT NULL    sortkey, 
                                                                 user_id        INT                    NOT NULL, 
                                                                 level          VARCHAR, 
                                                                 song_id        VARCHAR                NOT NULL    distkey, 
                                                                 artist_id      VARCHAR                NOT NULL, 
                                                                 session_id     INT, 
                                                                 location       VARCHAR, 
                                                                 user_agent     VARCHAR                );""")

    songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT
                                stg_events.ts as start_time,
                                stg_events.userid as user_id,
                                stg_events.level as level,
                                stg_songs.song_id as song_id,
                                stg_songs.artist_id as artist_id,
                                stg_events.sessionid as session_id,
                                stg_events.location as location,
                                stg_events.useragent as user_agent
                            FROM
                                staging_events as stg_events
                            JOIN
                                staging_songs as stg_songs
                            ON
                                (stg_events.artist = stg_songs.artist_name
                                 AND
                                 stg_events.song = stg_songs.title
                                 AND 
                                 stg_events.length = stg_songs.duration)""")

    
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)