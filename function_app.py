import logging
import azure.functions as func
import json
import os
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
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
