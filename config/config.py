from dotenv import load_dotenv
import os

load_dotenv()

TMDB_API_KEY = os.getenv("API_KEY")
DB_CONN_STRING = os.getenv("SQL_CONN")
log_path = os.getenv("LOG_PATH")
cleaned_movies_path = os.getenv("CLEANED_MOVIES_PATH")
raw_movies_path = os.getenv("RAW_MOVIES_PATH")
genres_path = os.getenv("GENRES_PATH")
production_companies_path = os.getenv("PRODUCTION_COMPANIES_PATH")
