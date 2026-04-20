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
SELECT c.customer_id, c.name, c.age, c.contact_number, c.email, w.genre, w.session_id
FROM customers c
JOIN watch_sessions w
  ON c.customer_id = w.customer_id
WHERE w.genre = 'Action'
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
