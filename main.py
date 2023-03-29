import os
import time
from os import system

import pandas as pd
import requests
import base64

from pyspark.sql.functions import col
from pyspark.sql import SparkSession


def get_path(num: int):
    return f"/Users/madslun/Documents/Programmering/Project work - BDA/data/MyData 3/endsong_{num}.json"


def get_all_files():
    path_list = []
    print(type(path_list))
    for i in range(0, 10):
        path_list.insert(i, get_path(i))
    return path_list


def pre_process_ids(ids: list):
    out = ""
    for s in ids:
        out += s[0].removeprefix("spotify:track:") + ","

    out = out.removesuffix(",")
    return out


def get_access_token():
    client_id = ""
    client_secret = ""
    url = 'https://accounts.spotify.com/api/token'

    ret = requests.post(url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    global access_token
    access_token = ret.json()['access_token']


def get_multiple_features(ids: str):
    url = " https://api.spotify.com/v1/audio-features?ids="
    ret = requests.get(url=url + f'{ids}',
                       headers={"Authorization": f"Bearer {access_token}",
                                },

                       )

    if ret.status_code != 200:
        print(f"Got {ret.text} on multiple features. Retrying in 5 seconds...")
        time.sleep(5)
        return get_multiple_features(ids)

    if len(ids.split(',')) != len(ret.json()['audio_features']):
        print(f"Did not get the same amount on {ids} at multiple features")
        print(ret.json())

    danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature = [], [], [], [], [], [], [], [], [], [], [], []
    for feature in ret.json()['audio_features']:

        if feature is None:
            danceability.append(None)
            energy.append(None)
            key.append(None)
            loudness.append(None)
            mode.append(None)
            speechiness.append(None)
            acousticness.append(None)
            instrumentalness.append(None)
            liveness.append(None)
            valence.append(None)
            tempo.append(None)
            time_signature.append(None)
            continue

        danceability.append(feature['danceability'])
        energy.append(feature['energy'])
        key.append(feature['key'])
        loudness.append(feature['loudness'])
        mode.append(feature['mode'])
        speechiness.append(feature['speechiness'])
        acousticness.append(feature['acousticness'])
        instrumentalness.append(feature['instrumentalness'])
        liveness.append(feature['liveness'])
        valence.append(feature['valence'])
        tempo.append(feature['tempo'])
        time_signature.append(feature['time_signature'])

    return danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature


def get_multiple_tracks(ids: str):
    url = "https://api.spotify.com/v1/tracks?ids=" + f'{ids}'
    ret = requests.get(url=url,
                       headers={"Authorization": f"Bearer {access_token}",
                                },

                       )
    if ret.status_code == 429:
        print(f"Too many requests! Waiting {ret.headers['retry-after']}")
        time.sleep(int(ret.headers['retry-after']))
        get_access_token()
        return get_multiple_tracks(ids)



    elif ret.status_code != 200:
        print(f"Got {ret.text} on multiple tracks. Retrying in 5 seconds...")
        time.sleep(5)
        return get_multiple_tracks(ids)

    if len(ids.split(',')) != len(ret.json()['tracks']):
        print(f"Did not get the same amount on {ids} at multiple tracks")
        print(ret.json())

    popularities, album_ids, album_release_dates, durations = [], [], [], []
    for track in ret.json()['tracks']:
        popularities.append(track['popularity'])
        album_ids.append(track['album']['id'])
        album_release_dates.append(track['album']['release_date'])
        durations.append(track['duration_ms'])

    return popularities, album_ids, album_release_dates, durations


def get_multiple_albums(ids: str):
    url = "https://api.spotify.com/v1/albums?ids="

    ret = requests.get(url=url + f'{ids}',
                       headers={"Authorization": f"Bearer {access_token}",
                                })

    genres = []
    for album in ret.json()['albums']:
        genres.append(album['genres'])

    return genres


def batch_extract(all_ids):
    batch_size = 50

    track_ids, popularities, album_ids, album_release_dates, durations = [], [], [], [], []
    danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature = [], [], [], [], [], [], [], [], [], [], [], []

    for i in range(0, len(all_ids), batch_size):

        curr_ids = all_ids[i:i + batch_size]
        curr_ids = pre_process_ids(curr_ids)
        pop, a_id, a_date, dur = get_multiple_tracks(curr_ids)

        assert len(curr_ids.split(",")) == len(pop) == len(a_id) == len(a_date) == len(dur)

        track_ids += curr_ids.split(",")
        popularities += pop
        album_ids += a_id
        album_release_dates += a_date
        durations += dur

        dan, ene, ke, lou, mod, spe, aco, ins, liv, val, tem, tim = get_multiple_features(curr_ids)
        danceability += dan
        energy += ene
        key += ke
        loudness += lou
        mode += mod
        speechiness += spe
        acousticness += aco
        instrumentalness += ins
        liveness += liv
        valence += val
        tempo += tem
        time_signature += tim

        # TODO: add length check on all in rang
        #  Add track id, remove square brackets of date

        if i % 1000 == 0:
            print(i)

    data = {'all_ids': track_ids,
            'popularities': popularities,
            'album_ids': album_ids,
            'album_release_dates': album_release_dates,
            'durations': durations,
            'danceability': danceability,
            "energy": energy,
            "key": key,
            'loudness': loudness,
            'mode': mode,
            'speechiness': speechiness,
            'acousticness': acousticness,
            'instrumentalness': instrumentalness,
            'liveness': liveness,
            'valence': valence,
            'tempo': tempo,
            'time_signature': time_signature
            }

    df1 = pd.DataFrame(data)
    df1.to_csv("track_data.csv")


def collect_data():
    # loop through data here
    path_list = get_all_files()
    spark = SparkSession.builder.appName("Spotify-data").getOrCreate()

    df = spark.read \
        .option("multiline", "true") \
        .json(path_list)

    a = df.where(col('spotify_track_uri').isNotNull()).select('spotify_track_uri').distinct().collect()
    batch_extract(a)


if __name__ == '__main__':
    global access_token
    access_token = None
    if access_token is None:
        get_access_token()

    collect_data()
