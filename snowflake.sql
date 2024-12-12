
SELECT CURRENT_ACCOUNT();


create database spotify_db;

create or replace storage integration s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::051826725028:role/spark_snowflake_role'
  STORAGE_ALLOWED_LOCATIONS=('s3://spotify-etl-data-mudasir')
  comment ='Creating connection to S3 ';

  DESC integration s3_init;--change the aws user arn and externalId in aws


CREATE OR REPLACE file format csv_fileformat
      type=csv
      field_delimiter=','
      skip_header=1
      null_if=('NULL','null')
      empty_field_as_null=TRUE;
      

 CREATE OR REPLACE stage spotify_staging
   URL='s3://spotify-etl-data-mudasir/transformed_data/'
   STORAGE_INTEGRATION=s3_init
   FILE_FORMAT=csv_fileformat;



 list @spotify_staging/album_data;


 CREATE OR REPLACE TABLE tbl_album(
     album_id STRING,
     album_name STRING,
     album_release_date DATE,
     album_total_tracks INT,
     album_url STRING
     
    
 ) 
 
CREATE OR REPLACE TABLE tbl_artists(
      artist_id STRING,
      artist_name STRING,
      artist_url STRING
 
 )
 


  CREATE OR REPLACE TABLE tbl_songs(
     song_name STRING,
     song_id STRING,
     duration_ms INT,
     song_url STRING,
     song_popularity INT,
     album_id STRING,
     artist_id STRING,
     song_added_at DATE
  )
 
COPY INTO tbl_songs
FROM @spotify_staging/songs_data/songs_transformed_2024-12-12/

COPY INTO tbl_songs
FROM @spotify_staging/songs_data/songs_transformed_2024-12-12/




select * from tbl_songs;

--create snowpipe
CREATE OR REPLACE SCHEMA PIPE;

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_album_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_staging/album_data/;

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_artists_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_staging/artist_data/;

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_songs_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_staging/songs_data/;

--Event creation
DESC pipe pipe.tbl_songs_pipe

DESC pipe pipe.tbl_album_pipe

DESC pipe pipe.tbl_artists_pipe

--testing 

SELECT COUNT(*) FROM TBL_ALBUM;

SELECT COUNT(*) FROM TBL_SONGS;

SELECT COUNT(*) FROM TBL_ARTISTS;
  