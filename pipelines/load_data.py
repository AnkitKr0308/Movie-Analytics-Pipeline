import pandas as pd
from config.config import DB_CONN_STRING, production_companies_path, raw_movies_path, cleaned_movies_path, genres_path
from sqlalchemy import create_engine, text
from utils.logger import write_log

engine = create_engine(DB_CONN_STRING)

logger = write_log()


def load_movies():
    # Merging data from staging table to movies table
    merge_movies_query = """
        MERGE INTO movies AS target
        USING staging.movies AS source
        ON target.id=source.id

        WHEN NOT MATCHED
        THEN
            INSERT (id, title, release_date, vote_average, vote_count)
            VALUES(source.id, source.title, source.release_date,
                    source.vote_average, source.vote_count)

        WHEN MATCHED AND (
            ISNULL(source.title, '') <> ISNULL(target.title, '') OR
            ISNULL(source.release_date, '1800-01-01') <> ISNULL(target.release_date, '1800-01-01') OR
            ISNULL(source.vote_average, 0) <> ISNULL(target.vote_average, 0) OR
            ISNULL(source.vote_count, 0) <> ISNULL(target.vote_count, 0)
        )
        THEN
            UPDATE SET
                title=source.title,
                release_date=source.release_date,
                vote_average=source.vote_average,
                vote_count=source.vote_count

        OUTPUT
            $action, inserted.id, deleted.id;
    """
    # loading data into staging.movies table
    try:
        logger.info("Reading data from cleaned_movie.csv file")
        df_movies = pd.read_csv(
            cleaned_movies_path)
        # print('Updated movie details into staging.movies table...')
        logger.info("Loading data into 'staging.movies' table...")
        df_movies.drop_duplicates(subset=["id"], inplace=True)
        # Loading the data into staging table
        df_movies.to_sql("movies", engine, schema="staging",
                         if_exists="replace", index=False)

        logger.info("Successfully loaded data into 'staging.movies' table.")
        # print('staging.movies tables updated')
        # Instead of using merge, join query can also be used to load data from staging table to movies table

        # loading data into dbo.movies table

        # df_movies.to_sql("movies", engine, if_exists="append", index=False)
        # executing query to load data into movies table
        with engine.begin() as conn:
            # print('Updating data into movies table...')
            logger.info(
                "Loading data from staging table to 'dbo.movies' table...")
            result = conn.execute(text(merge_movies_query))
            # fetchall() is a method on the result of a database query that retrieves all rows returned by that query and returns them as a list of tuples.
            changes = result.fetchall()

            # gets the count of inserted and updated rows in SQL table
            inserted = len([row for row in changes if row[0] == "INSERT"])
            updated = len([row for row in changes if row[0] == "UPDATE"])

        # print('Movies table updated...')
        logger.info("Successfully loaded data into 'dbo.movies' table.")
        logger.info(
            f"Total number of rows inserted into dbo.movies table: {inserted}")
        logger.info(
            f"Total number of rows updated into dbo.movies table: {updated}")
        # print('Rows inserted:', inserted)
        # print('Rows updated:', updated)

        # with engine.begin() as conn:
        #     print('Dropping staging.movies table')
        #     conn.execute(text("DROP TABLE staging.movies"))
        #     print('staging.movies table deleted')
    except Exception as e:
        logger.error(f"Movies pipeline failed with error: {e}")


def load_genres():
    merge_genre_query = """
        MERGE INTO genres AS target
        USING staging.genres AS source
        ON source.genre_id=target.genre_id

        WHEN NOT MATCHED
        THEN
            INSERT(genre_id, genre_name)
            VALUES(source.genre_id, source.genre_name)

        WHEN MATCHED AND(
            ISNULL(source.genre_name, '') <> ISNULL(target.genre_name,'')
        )
        THEN
            UPDATE SET
                genre_name=source.genre_name

        OUTPUT
            $action, inserted.genre_id as new_id, deleted.genre_id as old_id;
    """

    merge_movie_genre_query = """
        MERGE INTO movie_genre AS target
        USING staging.genres AS source
        ON source.genre_id = target.genre_id
            AND source.movie_id = target.movie_id

        WHEN NOT MATCHED
        THEN
            INSERT (genre_id, movie_id)
            VALUES(source.genre_id, source.movie_id)

        OUTPUT
            $action, inserted.genre_id;
    """
    # loading data into staging.genres table
    try:
        logger.info("Reading data from genres.csv file")
        df_genres = pd.read_csv(
            genres_path)
        logger.info("Loading data into 'staging.genres' table...")
        df_genres.drop_duplicates(subset=["genre_id"], inplace=True)
        df_genres.to_sql("genres", engine, schema="staging",
                         if_exists="replace", index=False)
        logger.info("Successfully loaded data into 'staging.genres' table")

        # loading data into 'dbo.genres' table
        logger.info("Loading data into 'dbo.genres' table")
        with engine.begin() as conn:
            result = conn.execute(text(merge_genre_query))
            changes = result.fetchall()

            inserted = len([row for row in changes if row[0] == "INSERT"])
            updated = len([row for row in changes if row[0] == "UPDATE"])

        logger.info("Successfully loaded data into 'dbo.genres' table")
        logger.info(f"Total number of rows inserted: {inserted}")
        logger.info(f"Total number of rows updated: {updated}")

        # loading into dbo.movie_genre table

        logger.info("Loading into 'dbo.movie_genre' table...")
        with engine.begin() as conn:
            result = conn.execute(text(merge_movie_genre_query))
            changes = result.fetchall()

            inserted = len([row for row in changes if row[0] == "INSERT"])
        logger.info("Successfully loaded data into 'dbo.movie_genre' table.")
        logger.info(f"Total rows inserted:{inserted}")

    except Exception as e:
        logger.error(f"Genres pipeline failed with error: {e}")


def load_companies():

    merge_production_companies_query = """
        MERGE INTO production_companies AS target
        USING staging.production_companies AS source
        ON source.company_id = target.company_id

        WHEN NOT MATCHED
        THEN
            INSERT(company_id, company_name)
            VALUES(source.company_id, source.company_name)

        WHEN MATCHED AND (
            ISNULL(source.company_name, '') <> ISNULL(target.company_name,'')
        )
        THEN
            UPDATE SET
                company_name=source.company_name

        OUTPUT
            $action, inserted.company_id, deleted.company_id;
    """

    merge_movie_company_query = """
        MERGE INTO movie_companies AS target
        USING staging.production_companies AS source
        ON source.movie_id = target.movie_id
            AND source.company_id = target.company_id

        WHEN NOT MATCHED
        THEN
            INSERT (company_id, movie_id)
            VALUES (source.company_id, source.movie_id)
        
        OUTPUT
            $action, inserted.company_id;
    """
    # loading data into staging.production_companies table
    try:
        logger.info("Reading data from production_companies.csv file")
        df_companies = pd.read_csv(
            production_companies_path)
        logger.info("Loading data into 'staging.production_companies' table")
        df_companies.drop_duplicates(subset=["company_id"], inplace=True)
        df_companies.to_sql("production_companies", engine,
                            schema="staging", if_exists="replace", index=False)
        logger.info(
            "Successfully loaded data into 'staging.production_companies' table")

    # loading data into dbo.production_companies table

        logger.info("Loading into 'dbo.production_companies' table...")
        with engine.begin() as conn:
            result = conn.execute(text(merge_production_companies_query))
            changes = result.fetchall()

            inserted = len([row for row in changes if row[0] == "INSERT"])
            updated = len([row for row in changes if row[0] == "UPDATE"])

        logger.info(
            "Successfully loaded data into 'dbo.production_companies' table")
        logger.info(f"Total number of rows inserted: {inserted}")
        logger.info(f"Total number of rows updated: {updated}")

    # loading into movie_companies table

        logger.info("Loading into 'dbo.movie_companies' table...")
        with engine.begin() as conn:
            result = conn.execute(text(merge_movie_company_query))
            changes = result.fetchall()

            inserted = len([row for row in changes if row[0] == "INSERT"])

        logger.info(
            "Successfully loaded data into 'dbo.movie_companies' table.")
        logger.info(f"Total number of rows inserted: {inserted}")
    except Exception as e:
        logger.error(f"Production Companies pipeline failed with error: {e}")


def load_data():
    load_movies()
    load_genres()
    load_companies()
