import pandas as pd
from config.config import DB_CONN_STRING
from sqlalchemy import create_engine, text

engine = create_engine(DB_CONN_STRING)

df_movies = pd.read_csv("data\\cleaned_movie.csv")
print('Writing to Movie Staging table...')

# Loading the data into staging table
df_movies.to_sql("movies_staging", engine, schema="staging",
                 if_exists="replace", index=False)

# Instead of using merge, join query can also be used to load data from staging table to movies table

# Merging data from staging table to main table
merge_query = """
    MERGE INTO movies AS target
    USING staging.movies_staging AS source
    ON target.id=source.id

    WHEN NOT MATCHED THEN
        INSERT (id, title, release_date, vote_average, vote_count)
        VALUES(source.id, source.title, source.release_date, source.vote_average, source.vote_count)

    WHEN MATCHED THEN
        UPDATE SET
            title=source.title,
            release_date=source.release_date,
            vote_average=source.vote_average,
            vote_count=source.vote_count

    OUTPUT
        $action, inserted.id, deleted.id;
"""


# df_movies.to_sql("movies", engine, if_exists="append", index=False)
# executing query to load data into movies table
with engine.begin() as conn:
    result = conn.execute(text(merge_query))
    # fetchall() is a method on the result of a database query that retrieves all rows returned by that query and returns them as a list of tuples.
    changes = result.fetchall()

    # gets the count of inserted and updated rows in SQL table
    inserted = len([row for row in changes if row[0] == "INSERT"])
    updated = len([row for row in changes if row[0] == "UPDATe"])

print('Movies table updated...')
print('Rows inserted:', inserted)
print('Rows updated:', updated)

with engine.begin() as conn:
    conn.execute(text("DROP TABLE staging.movies_staging"))
