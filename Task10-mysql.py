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
SELECT * FROM watch_sessions WHERE (mood1 = "excited" OR mood2 = "excited") AND mood1 != "bored" AND mood2 != "bored"
"""

for i in range(10):
    start = time.perf_counter()
    cursor.execute(query)
    rows = cursor.fetchall()  
    end = time.perf_counter()
    times.append(end - start)

    print(f"Iteration {i+1} first record: {rows[0]}")


average_time = sum(times) / len(times)

print("Run times:", times)
print("Average:", average_time)

# Cleanup
cursor.close()
conn.close()
# %%
