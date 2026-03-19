from config.config import TMDB_API_KEY
from pandas.errors import EmptyDataError
import requests
import pandas as pd
import os
import time


# What is import requests?
# requests is a Python library (module) used to send HTTP requests.
# HTTP requests are how your Python program talks to web servers, APIs, or websites.


file_path = "data\\movies.csv"
try:
    df_existing_movie = pd.read_csv(file_path)
    existing_ids = set(df_existing_movie["id"].tolist())
except (FileNotFoundError, EmptyDataError, KeyError):
    df_existing_movie = pd.DataFrame()
    existing_ids = set()


def fetch_movies(page):
    try:
        url = f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&page={page}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(url, headers=headers, timeout=10)
        if (response.status_code == 404):
            return None  # skipping non-existing movies
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error for page {page}: {e}")


new_movies = []
page = 1
while (len(new_movies) < 100):
    try:
        print(f'Fetching page {page}...')
        movies = fetch_movies(page)
        if not movies:
            continue
        for movie in movies:
            if (movie['id'] not in existing_ids):
                new_movies.append(movie)
                existing_ids.add(movie['id'])

            if (len(new_movies) >= 100):
                break
        time.sleep(0.3)
        page += 1
    except Exception as e:
        print(f'Error on page {page}: {e}')

if new_movies:
    df_new_movies = pd.DataFrame(new_movies)
    df_final = pd.concat([df_existing_movie, df_new_movies], ignore_index=True)
    df_final.to_csv("data\\movies.csv", index=False)
    print(
        f"New movies added: {len(df_new_movies)}. Total movies now: {len(df_final)}")
