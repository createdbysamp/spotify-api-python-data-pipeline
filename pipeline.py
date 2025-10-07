from requests.exceptions import HTTPError
from pandas import DataFrame
from dotenv import load_dotenv
from requests import post, get

import logging
import argparse
import requests
import pandas as pd
import sqlite3
import os
import base64
import json
from typing import List
from pprint import pprint


# from utilities import flatten_dimensions, flatten_meta, camel_to_snake


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='pipeline_log.log' # where to log to
)

# constants ------------------------------------------------------------------|
API_ENDPOINT = "https://api.spotify.com/v1/artists/0FneT6GwedlczRJrLsDD9r"
# DB_FILE = "southstate_top_songs.db"
# DB_FILE = "data/nickleback_top_songs.db"
# TABLE_NAME = "nickleback_top_songs"
# CREATE_TABLE_SQL = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id TEXT PRIMARY KEY, name TEXT NOT NULL, album_name TEXT, popularity INTEGER, duration_ms INTEGER, explicit BOOLEAN, preview_url TEXT);"

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# print(client_id, client_secret)


# ARGE PARSE. ------------------------------------------------------------------|
def get_args():
    # creation of parser object
    parser = argparse.ArgumentParser(
        description="Run the ETL pipeline with custom input."
    )
    # the parser is now ready to accept arguments
    parser.add_argument(
        "--artist",
        type=str,
        required=True,
        help="The name of the artist to search for.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data.db",
        required=True,
        help="the name of the db to save to!",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="etl_output",
        help="the name of the SQLite table to load the data into.",
    )

    return parser


# SPOTIFY API ------------------------------------------------------------------|
def get_token():
    logging.info(f"Attempting to get Spotify token.")
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "client_credentials"}

    result = post(url, headers=headers, data=data)

    # return JSON DATA in field "content"
    # convert to python dictionary to be able to access it
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    
    logging.info(f"token retrevial successful! you've got sonic coins now!!")
    return token


def get_auth_header(token):
    return {"Authorization": "Bearer " + token}


def search_for_artist(token, artist_name):
    logging.info(f"searching for artist! {artist_name}")
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"

    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]
    if len(json_result) == 0:
        print("No artist exists with this name")
        return None
    
    logging.info(f"artist found!! nice!")

    return json_result[0]


# functions ------------------------------------------------------------------|
def get_songs_by_artist(token, artist_id):
    # logging.info(f"pulling up songs by artist.")

    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
    try: 
        headers = get_auth_header(token)
        result = get(url, headers=headers)
        json_result = json.loads(result.content)["tracks"]
        logging.info(f"songs by artist found!")
        return json_result
    except requests.exceptions.RequestException as e: 
        logging.critical(f"FATAL EXTRACTION ERROR: Network or API failure for token. Details {e}")
        return None


def transform(product_list: List[dict]) -> DataFrame:
    """Flattens the product list and selects/cleans key fields."""

    df = pd.json_normalize(product_list, sep="_")

    df = df[
        [
            "id",
            "name",
            "album_name",
            "popularity",
            "duration_ms",
            "explicit",
            "preview_url",
        ]
    ]

    string_cols = ["id", "name", "album_name", "preview_url"]
    df[string_cols] = df[string_cols].apply(lambda x: x.str.strip())

    df["preview_url"] = df["preview_url"].fillna("Unknown")

    # df["explicit"] = df["explicit"].replace({True: "explicit", False: "clean"})

    not_explicit = df.loc[df["explicit"] == False, ["name", "album_name", "explicit"]]

    print(not_explicit)

    return df


def load(dataframe: pd.DataFrame, db_path: str, table_name: str):
    """Loads the final DataFrame into a SQLite database table."""
    # Use sqlite3.connect() and df.to_sql() here
    CREATE_TABLE_SQL = f"CREATE TABLE IF NOT EXISTS {table_name} (id TEXT PRIMARY KEY, name TEXT NOT NULL, album_name TEXT, popularity INTEGER, duration_ms INTEGER, explicit BOOLEAN, preview_url TEXT);"

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        print("table created!!")

    logging.warning("Pipeline started with default settings.")
    with sqlite3.connect(db_path) as conn:
        dataframe.to_sql(name=table_name, con=conn, if_exists="append", index=False)
        print("data loaded!!")
    logging.info("Pipeline completed successfully!!")


# main ------------------------------------------------------------------|
def main(artist, table_name, db_path):
    token = get_token()
    result = search_for_artist(token, artist)
    print(result["name"])
    artist_id = result["id"]
    songs = get_songs_by_artist(token, artist_id)

    song_df = transform(songs)
    loaded_df = load(song_df, db_path, table_name)
    return


if __name__ == "__main__":
    # Call your functions in sequence: E -> T -> L
    parser = get_args()
    args = parser.parse_args()

    main(args.artist, args.table, args.db)
