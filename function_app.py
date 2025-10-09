import logging
import azure.functions as func
import json
import os
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
import time
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.queue import QueueClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */2 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def spotifyApiDataExtractFunc(myTimer: func.TimerRequest) -> None:
    logging.info("Spotify ETL Timer Trigger function started...")

    # Spotify credentials
    spotify_client_id = os.getenv('spotify_client_id')
    spotify_client_secret = os.getenv('spotify_client_secret')

    if not spotify_client_id or not spotify_client_secret:
        logging.error("Spotify credentials not found in environment variables.")
        return

    #  Spotify API client setup
    client_credential_manager = SpotifyClientCredentials(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)

    # Fetch playlist data
    playlist_link = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"
    playlist_id = playlist_link.split("/")[-1].split("?")[0]
    playlist_uri = f"spotify:playlist:{playlist_id}"

    spotify_data = sp.playlist_tracks(playlist_uri)
    spotify_json = json.dumps(spotify_data)

    logging.info("Spotify data extracted successfully")

    #Prepare filename and Blob details
    file_name = f"spotify_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    container_name = "rawdata"
    blob_name = f"to_processed/{file_name}"

    #Upload to Azure Blob Storage
    azure_connection_string = os.getenv("az_connection_string")
    if not azure_connection_string:
        logging.error("Azure Storage connection string not found in environment variables.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    #(Removed unnecessary download before upload)
    blob_client.upload_blob(spotify_json, overwrite=True)

    logging.info(f"Uploaded to Azure Blob Storage: {container_name}/{blob_name}")


# Helper functions
def album(data):
    album_list = []
    for row in data['items']:
        album_info = row['track']['album']
        album_element = {
            'album_id': album_info['id'],
            'name': album_info['name'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'url': album_info['external_urls']['spotify']
        }
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_dict = {
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            }
            artist_list.append(artist_dict)
    return artist_list

def songs(data):
    song_list = []
    for row in data['items']:
        song_info = row['track']
        song_element = {
            'song_id': song_info['id'],
            'song_name': song_info['name'],
            'duration_ms': song_info['duration_ms'],
            'url': song_info['external_urls']['spotify'],
            'popularity': song_info.get('popularity', None),
            'song_added': row['added_at'],
            'album_id': song_info['album']['id'],
            'artist_id': song_info['album']['artists'][0]['id']
        }
        song_list.append(song_element)
    return song_list


def upload_dataframes(blob_service_client, df, path, filename):
    container_name = "transformeddata"  
    blob_name = f"{path}{filename}"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

def move_blobs(blob_service_client, container_name):
    
        container_client = blob_service_client.get_container_client(container_name)

        for blob in container_client.list_blobs(name_starts_with='to_processed/'):
            blob_name = blob.name
            new_blob_name = blob_name.replace('to_processed/','processed/',1)
            source_blob_client = container_client.get_blob_client(blob_name)
            dest_blob_client = container_client.get_blob_client(new_blob_name)
            
            dest_blob_client.start_copy_from_url(source_blob_client.url)
            logging.info(f"copied from {blob_name} to {new_blob_name}")

            source_blob_client.delete_blob()
            logging.info(f"deleted originnal : {blob_name}")


@app.blob_trigger(arg_name="myblob", path="rawdata/to_processed/{name}",
                               connection="az_connection_string") 
def BlobTriggerSpotifyETLFunc(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob "
                 f"Name: {myblob.name} "
                 f"Blob Size: {myblob.length} bytes")

    content = myblob.read()
    data = json.loads(content)

    # Transform data
    album_list = album(data)
    artist_list = artist(data)
    song_list = songs(data)

    album_df = pd.DataFrame(album_list).drop_duplicates(subset=['album_id'])
    artist_df = pd.DataFrame(artist_list).drop_duplicates(subset=['artist_id'])
    song_df = pd.DataFrame(song_list).drop_duplicates(subset=['song_id'])

    album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
    song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')

    conn_str = os.getenv('az_connection_string')
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    upload_dataframes(blob_service_client, album_df, "album_data/", f"album_{timestamp}.csv")
    upload_dataframes(blob_service_client, artist_df, "artist_data/", f"artist_{timestamp}.csv")
    upload_dataframes(blob_service_client, song_df, "songs_data/", f"songs_{timestamp}.csv")

    move_container_name = 'rawdata'
    move_blobs(blob_service_client,move_container_name)
    logging.info("Move Successfull")
