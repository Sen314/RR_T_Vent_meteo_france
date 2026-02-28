import psycopg2
conn = psycopg2.connect(
    dbname="weather_db",
    user="postgres",
    password="sxI6sfqn4KI0Eq56we9u",
    host="localhost",
    port="5432"
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())

cur.close()
conn.close()