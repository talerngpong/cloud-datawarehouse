from typing import List

from common import EtlConfig

# CONFIG - moved to `common.py`

# DROP TABLES

staging_events_table_drop = '''
    DROP TABLE IF EXISTS staging_events
'''
staging_songs_table_drop = '''
    DROP TABLE IF EXISTS staging_songs
'''
songplay_table_drop = '''
    DROP TABLE IF EXISTS songplays
'''
user_table_drop = '''
    DROP TABLE IF EXISTS users
'''
song_table_drop = '''
    DROP TABLE IF EXISTS songs
'''
artist_table_drop = '''
    DROP TABLE IF EXISTS artists
'''
time_table_drop = '''
    DROP TABLE IF EXISTS times
'''

# CREATE TABLES

staging_events_table_create = ('''
    CREATE TABLE staging_events (
        artist        TEXT      NULL,
        auth          TEXT      NULL,
        firstName     TEXT      NULL,
        gender        TEXT      NULL,
        itemInSession INT       NULL,
        lastName      TEXT      NULL,
        length        NUMERIC   NULL,
        level         TEXT      NULL,
        location      TEXT      NULL,
        method        TEXT      NULL,
        page          TEXT      NULL,
        registration  NUMERIC   NULL,
        sessionId     TEXT      NULL DISTKEY,
        song          TEXT      NULL,
        status        INT       NULL,
        ts            TIMESTAMP NULL, -- from epochmillisecs
        userAgent     TEXT      NULL,
        userId        TEXT      NULL
    )
''')

staging_songs_table_create = ('''
    CREATE TABLE staging_songs (
        num_songs        INT     NULL,
        artist_id        TEXT    NULL DISTKEY,
        artist_latitude  TEXT    NULL,
        artist_longitude TEXT    NULL,
        artist_location  TEXT    NULL,
        artist_name      TEXT    NULL,
        song_id          TEXT    NULL,
        title            TEXT    NULL,
        duration         NUMERIC NULL,
        year             INT     NULL
    )
''')

songplay_table_create = ('''
    CREATE TABLE songplays (
        songplay_id BIGINT    NOT NULL IDENTITY(1, 1),
        start_time  TIMESTAMP NOT NULL,
        user_id     TEXT      NULL,
        level       TEXT      NULL,
        song_id     TEXT      NULL,
        artist_id   TEXT      NULL,
        session_id  TEXT      NULL,
        location    TEXT      NULL,
        user_agent  TEXT      NULL,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (user_id)   REFERENCES users   (user_id),
        FOREIGN KEY (song_id)   REFERENCES songs   (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
''')

user_table_create = ('''
    CREATE TABLE IF NOT EXISTS users (
        user_id    TEXT NOT NULL,
        first_name TEXT NULL,
        last_name  TEXT NULL,
        gender     TEXT NULL,
        level      TEXT NULL,
        PRIMARY KEY (user_id)
    )
''')

song_table_create = ('''
    CREATE TABLE IF NOT EXISTS songs (
        song_id   TEXT    NOT NULL,
        title     TEXT    NULL,
        artist_id TEXT    NOT NULL,
        year      INT     NULL,
        duration  NUMERIC NULL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
    )
''')

artist_table_create = ('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL,
        name      TEXT,
        location  TEXT,
        latitude  TEXT,
        longitude TEXT,
        PRIMARY KEY (artist_id)
    )
''')

time_table_create = ('''
    CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP NOT NULL,
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    INT,
        PRIMARY KEY (start_time)
    )
''')


# STAGING TABLES


def build_copy_table_queries(etl_config: EtlConfig, dwh_iam_role_arn: str) -> List[str]:
    # Since we deal timestamp in `epochmillisecs` unit, we can apply that data conversion during COPY operation.
    # Ref: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat
    staging_events_copy = ('''
        copy staging_events
        from 's3://{manifest_bucket_name}/{event_data_manifest_key}'
        credentials 'aws_iam_role={iam_role_arn}'
        format as json 's3://{data_set_bucket_name}/{data_set_event_data_json_path_key}'
        timeformat as 'epochmillisecs'
        region '{region}'
        manifest;
    ''').format(
        manifest_bucket_name=etl_config.manifest.bucket_name,
        event_data_manifest_key=etl_config.manifest.event_data_key,
        iam_role_arn=dwh_iam_role_arn,
        region=etl_config.region_name,
        data_set_bucket_name=etl_config.data_set.bucket_name,
        data_set_event_data_json_path_key=etl_config.data_set.log_data_json_path_key
    )
    staging_songs_copy = ('''
        copy staging_songs
        from 's3://{manifest_bucket_name}/{song_data_manifest_key}'
        credentials 'aws_iam_role={iam_role_arn}'
        format as json 'auto'
        region '{region}'
        manifest;
    ''').format(
        manifest_bucket_name=etl_config.manifest.bucket_name,
        song_data_manifest_key=etl_config.manifest.song_data_key,
        iam_role_arn=dwh_iam_role_arn,
        region=etl_config.region_name
    )
    copy_table_queries = [
        staging_events_copy,
        staging_songs_copy
    ]

    return copy_table_queries


# FINAL TABLES

songplay_table_insert = ('''
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        se.ts AS start_time,
        se.userId AS user_id,
        se.level,
        s.song_id,
        a.artist_id,
        se.sessionId AS session_id,
        se.location,
        se.userAgent as user_agent
    FROM staging_events se
    JOIN songs s
        ON (s.title = se.song AND s.duration = se.length)
    JOIN artists a
        ON a.name = se.artist
''')

user_table_insert = ('''
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT
        DISTINCT userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE COALESCE(userId, '') <> ''
''')

song_table_insert = ('''
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT
        DISTINCT song_id AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE COALESCE(song_id, '') <> ''
''')

artist_table_insert = ('''
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT
        DISTINCT artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE COALESCE(artist_id, '') <> ''
''')

time_table_insert = ('''
    INSERT INTO times (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        DISTINCT ts AS start_time,
        extract(hour from ts) AS hour,
        extract(day from ts) AS day,
        extract(week from ts) AS week,
        extract(month from ts) AS month,
        extract(year from ts) AS year,
        extract(weekday from ts) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL
''')

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    time_table_create,
    song_table_create,
    songplay_table_create
]
drop_table_queries = [
    songplay_table_drop,
    song_table_drop,
    time_table_drop,
    artist_table_drop,
    user_table_drop,
    staging_songs_table_drop,
    staging_events_table_drop
]
insert_table_queries = [
    user_table_insert,
    artist_table_insert,
    time_table_insert,
    song_table_insert,
    songplay_table_insert
]
