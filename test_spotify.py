import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

load_dotenv()

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

print(f"Client ID: {client_id[:8]}...")
print(f"Client Secret: {client_secret[:8]}...")

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=client_id,
    client_secret=client_secret,
))

result = sp.search(q="artist:Drake", type="artist", limit=1)
artist = result["artists"]["items"][0]
print(f"\nConnected!")
print(f"Artist: {artist['name']}")
print(f"Popularity: {artist['popularity']}")
print(f"Followers: {artist['followers']['total']:,}")
