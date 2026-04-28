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

query = """
SELECT DISTINCT watch_date FROM watch_sessions
"""

for i in range(10):
    start = time.perf_counter()
    cursor.execute(query)
    rows = cursor.fetchall()  
    end = time.perf_counter()
    times.append(end - start)

    rows = sorted(rows)
    print(f"Iteration {i+1} first record: {rows[0]}")


average_time = sum(times) / len(times)

print("Run times:", times)
print("Average:", average_time)

# Cleanup
cursor.close()
conn.close()
# %%
