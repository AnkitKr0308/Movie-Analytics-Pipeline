from dotenv import load_dotenv
import os

load_dotenv()

TMDB_API_KEY = os.getenv("API_KEY")
DB_CONN_STRING = os.getenv("SQL_CONN")
