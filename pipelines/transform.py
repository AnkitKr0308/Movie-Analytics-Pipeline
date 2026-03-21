import pandas as pd
import ast
from utils.logger import write_log
from config import production_companies_path, raw_movies_path, cleaned_movies_path, genres_path


logger = write_log()


def get_movies_df():
    df_movies = pd.read_csv(raw_movies_path, usecols=[
        "id", "title", "release_date", "vote_average", "vote_count", "production_companies", "genres"])
    return df_movies


def transform_movies():
    try:
        logger.info('Cleaning and transforming movies...')
        df_movies = get_movies_df()
        # converting release_date datatype to datetime
        df_movies["release_date"] = pd.to_datetime(
            df_movies["release_date"], format="mixed")
        # removes any duplicate entries
        df_movies.drop_duplicates(subset=["id"], inplace=True)

        logger.info('Writing cleaned data into cleaned_movies.csv')
        df_movies.to_csv(
            cleaned_movies_path, columns=["id", "title", "release_date", "vote_average", "vote_count"], index=False)
    except Exception as e:
        logger.error("Error transforming movies: {e}")
        raise


def transform_genres():
    try:
        logger.info('Cleaning and transforming genres...')
        df_movies = get_movies_df()
        df_movies["genres"] = df_movies["genres"].apply(
            lambda x: ast.literal_eval(x) if pd.notna(x) else [])

        genres_list = []
        for _, row in df_movies.iterrows():
            movie_id = row["id"]
            for genre in row['genres']:
                genres_list.append({
                    "movie_id": movie_id,
                    "genre_id": genre["id"],
                    "genre_name": genre["name"]
                })
        df_genres = pd.DataFrame(genres_list)

        logger.info('Writing cleaned data into genres.csv')
        df_genres.to_csv(
            genres_path, index=False)

    except Exception as e:
        logger.error("Error in transforming genres:", e)
        raise


def transform_productions():
    try:
        logger.info('Cleaning and transforming into production companies...')
        df_movies = get_movies_df()
        df_movies["production_companies"] = df_movies["production_companies"].apply(
            # if value would be json it would convert into python object else will return []
            lambda x: ast.literal_eval(x) if pd.notna(x) else [])

        companies_list = []
        for _, row in df_movies.iterrows():  # iterrows iterates through each row in a loop
            movie_id = row["id"]
            for comp in row["production_companies"]:
                companies_list.append({
                    "movie_id": movie_id,
                    "company_id": comp["id"],
                    "company_name": comp["name"]
                })
        df_companies = pd.DataFrame(companies_list)

        logger.info('Writing cleaned data into production_companies.csv')
        df_companies.to_csv(
            production_companies_path, index=False)
    except Exception as e:
        logger.error("Error in transforming production companies:", e)
        raise


def transform_data():
    logger.info("Starting Transformation...")
    try:
        transform_movies()
        transform_genres()
        transform_productions()
    except Exception as e:
        logger.error(f"Error running Transformation pipeline: {e}")
        raise
    logger.info("Cleanup and transformation of data is completed.")
