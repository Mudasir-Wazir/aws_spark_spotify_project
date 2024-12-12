import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Fetch client credentials from environment variables
    client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')

    # Initialize Spotify client credentials manager
    auth_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Fetch data (e.g., tracks from a public playlist)
    playlist = 'https://open.spotify.com/playlist/6VOedaf3eNWDOVpa9Qdlvg'
    playlist_URI = playlist.split("/")[-1]
    
    try:
        data = sp.playlist_tracks(playlist_URI)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error fetching playlist tracks: {str(e)}")
        }

    # Save data to S3
    s3_client = boto3.client('s3')
    bucket_name = "spotify-etl-data-mudasir"  # Replace with your S3 bucket name
    filename = "spotify_raw_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key="raw_data/to_be_processed_data/" + filename,
            Body=json.dumps(data)
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error saving to S3: {str(e)}")
        }

    # Set up AWS Glue and start a job
    glue = boto3.client("glue")
    gluejobname = "spotify_trnfm_job"  # Your Glue job name

    try:
        # Start the Glue job
        run_id = glue.start_job_run(JobName=gluejobname)
        print(f"Glue job started with RunId: {run_id['JobRunId']}")
        
        # Check the Glue job status (optional, adds delay for job completion)
        status = glue.get_job_run(JobName=gluejobname, RunId=run_id['JobRunId'])
        print(f"Glue Job Status: {status['JobRun']['JobRunState']}")
    except glue.exceptions.ClientError as e:
        print(f"Glue Client Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error triggering Glue job: {str(e)}")
        }
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Unexpected error: {str(e)}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f"Top tracks fetched and saved to S3 successfully at {bucket_name}/raw_data/to_be_processed_data/{filename} and Glue job triggered.")
    }
