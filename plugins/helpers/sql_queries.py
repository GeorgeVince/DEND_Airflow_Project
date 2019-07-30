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

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

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