import pandas as pd
import ast

df_movies = pd.read_csv("data\\movies.csv", usecols=[
                        "id", "title", "release_date", "vote_average", "vote_count", "production_companies", "genres"])
# converting release_date datatype to datetime
df_movies["release_date"] = pd.to_datetime(
    df_movies["release_date"], format="mixed")
# removes any duplicate entries
df_movies.drop_duplicates(subset=["id"], inplace=True)

print('Writing movie...')
df_movies.to_csv(
    "D:\\Ankit\\Data Engineering\\Movie Analytics Pipeline\\data\\cleaned_movie.csv", columns=["id", "title", "release_date", "vote_average", "vote_count"], index=False)

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

print('Writing genres...')
df_genres.to_csv(
    "D:\\Ankit\\Data Engineering\\Movie Analytics Pipeline\\data\\genres.csv", index=False)


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

print('Writing production companies...')
df_companies.to_csv(
    "D:\\Ankit\\Data Engineering\\Movie Analytics Pipeline\\data\\production_companies.csv", index=False)
