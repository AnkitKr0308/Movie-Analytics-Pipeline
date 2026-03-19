import pandas as pd
import ast

df_movies = pd.read_csv("data\\movies.csv", usecols=["id","title","release_date","vote_average","vote_count"])
df_movies["release_date"]=pd.to_datetime(df_movies["release_date"],format="mixed") # converting release_date datatype to datetime
df_movies.drop_duplicates(subset=["id"], inplace=True) # removes any duplicate entries
print(df_movies.head())

# df_movies["genres"] = df_movies["genres"].apply(ast.literal_eval)
df_movies["production_companies"] = df_movies["production_companies"].apply(ast.literal_eval)

companies_list=[]
for _, row in df_movies.iterrows(): # iterrows iterates through each row in a loop
    movie_id=row["id"]
    for comp in row["production_companies"]:
        companies_list.append({
            "movie_id":movie_id,
            "company_id":comp["id"],
            "company_name":comp["name"]
        })
df_companies=pd.DataFrame(companies_list)
print(df_companies)