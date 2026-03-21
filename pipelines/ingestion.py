from config import TMDB_API_KEY, raw_movies_path
from pandas.errors import EmptyDataError
import requests
import pandas as pd
import os
import time
from utils.logger import write_log


# What is import requests?
# requests is a Python library (module) used to send HTTP requests.
# HTTP requests are how your Python program talks to web servers, APIs, or websites.


logger = write_log()


def load_existing_movies():
    try:
        df = pd.read_csv(raw_movies_path)
        existing_ids = set(df["id"].to_list())
        last_date = df["release_date"].max()
        return df, existing_ids, last_date
    except (FileNotFoundError, EmptyDataError, KeyError):
        return pd.DataFrame(), set(), "2000-01-01"


def fetch_movies(page):
    try:
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_API_KEY}&sort_by=created_at.desc&page={page}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching movie list page {page}: {e}")


def fetch_movie_details(movie_id):
    try:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching details for movie {movie_id}: {e}")
        return None


def ingestion():

    logger.info("Ingestion Started...")

    df_existing, existing_ids, last_date = load_existing_movies()

    new_movies = []
    page = 1
    while (len(new_movies) < 100):
        try:
            logger.info(f'Fetching page {page}...')

            movies = fetch_movies(page)
            if not movies:
                break
            for movie in movies:
                movie_id = movie["id"]

                if movie_id in existing_ids:
                    continue

                details = fetch_movie_details(movie_id)

                if not details:
                    continue
                new_movies.append(details)

                existing_ids.add(movie_id)

                if (len(new_movies) >= 100):
                    break

                time.sleep(0.3)
            page += 1

        except Exception as e:
            logger.error(f'Error on page {page}: {e}')

    if new_movies:
        df_new_movies = pd.DataFrame(new_movies)
        df_final = pd.concat(
            [df_existing, df_new_movies], ignore_index=True)
        df_final.to_csv(raw_movies_path, index=False)
        logger.info(
            f"New movies added: {len(df_new_movies)}. Total movies now: {len(df_final)}")


def run_ingestion():
    try:

        ingestion()
    except Exception as e:
        logger.error(f"Error running Ingestion pipeline: {e}")
        raise
