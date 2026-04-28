#%%
import mysql.connector
import time

# Connect to MySQL
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

    cursor.execute("SELECT COUNT(*) FROM watch_sessions_temp")
    watch_sessions_before = cursor.fetchall()
    print(f"Iteration {i+1} rows before delete: {watch_sessions_before[0]}")

    start = time.perf_counter()
    cursor.execute("DELETE FROM watch_sessions_temp WHERE mood1 IS NULL AND mood2 IS NULL")
    conn.commit()

    end = time.perf_counter()
    times.append(end - start)

    cursor.execute("SELECT COUNT(*) FROM watch_sessions_temp")
    watch_sessions_after = cursor.fetchall()
    print(f"Iteration {i+1} rows after delete: {watch_sessions_after[0]}")



average_time = sum(times) / len(times)

print("Run times:", times)
print("Average:", average_time)

# Cleanup
cursor.close()
conn.close()
# %%
