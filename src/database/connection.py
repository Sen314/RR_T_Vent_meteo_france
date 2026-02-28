import psycopg2
from psycopg2.extras import RealDictCursor
import os


def get_connection():
    return psycopg2.connect(
        dbname="weather_db",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )