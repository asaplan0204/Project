#%%
import mysql.connector
import time

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviesdb_sql"
)
cursor = conn.cursor()

times = []

for i in range(10):
    # --- CREATE TEMP COPY ---
    cursor.execute("DROP TABLE IF EXISTS watch_sessions_temp")
    cursor.execute("CREATE TABLE watch_sessions_temp LIKE watch_sessions")
    cursor.execute("INSERT INTO watch_sessions_temp SELECT * FROM watch_sessions")
    conn.commit()
    cursor.execute("SELECT pause_min1 FROM watch_sessions_temp WHERE session_id = 1")
    session_before = cursor.fetchone()[0]
    print(f"Iteration {i+1} pause_min1 before update: {session_before}")

    start = time.perf_counter()

    cursor.execute("UPDATE watch_sessions_temp SET pause_min1 = 50 WHERE session_id = 1")
    conn.commit()

    end = time.perf_counter()
    times.append(end - start)

    cursor.execute("SELECT pause_min1 FROM watch_sessions_temp WHERE session_id = 1")
    session_after = cursor.fetchone()[0]
    print(f"Iteration {i+1} pause_min1 before update: {session_after}")

average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

cursor.close()
conn.close()


# Restting pause_min1 to orginal value in watch_sessions
#%%
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviesdb_sql"
)
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS watch_sessions_temp")
cursor.execute("CREATE TABLE watch_sessions_temp LIKE watch_sessions")
cursor.execute("INSERT INTO watch_sessions_temp SELECT * FROM watch_sessions")
conn.commit()
cursor.execute("SELECT pause_min1 FROM watch_sessions_temp WHERE session_id = 1")
session_after = cursor.fetchone()[0]
print(f"Resetting pause_min1 to orginal value: {session_after}")

cursor.close()
conn.close()
# %%
