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



    

    