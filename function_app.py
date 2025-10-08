import logging
import azure.functions as func
import json
import os
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 20 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def spotifyApiDataExtractFunc(myTimer: func.TimerRequest) -> None:

        spotify_client_id = os.getenv('spotipy_client_id')
        spotify_client_secret = os.getenv('spotipy_client_secret')

        client_credential_manager = SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)
        playlist = sp.user_playlist('spotify')

        playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
        playlist_uri = playlist_link.split("/")[-1].split("?")[0]

        spotify_data = sp.playlist_tracks(playlist_uri)
        spotify_json = json.dumps(spotify_data)

        file_name = "spotify_raw" + str(datetime.now()) + '.json'

        azure_connection_string = os.getenv("az_connection_string")
        container_name = "rawdata"
        blob_name = f"to_processed/{file_name}"

        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        blob_data = blob_client.download_blob().content_as_text()

        blob_client.upload_blob(spotify_json, overwrite=True)


    