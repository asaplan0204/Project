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
    cursor.execute("DROP TABLE IF EXISTS customers_temp")
    cursor.execute("CREATE TABLE customers_temp LIKE customers")
    cursor.execute("INSERT INTO customers_temp SELECT * FROM customers")
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM customers_temp")
    customers_before = cursor.fetchall()
    print(f"Iteration {i+1} rows before delete: {customers_before[0]}")

    start = time.perf_counter()
    cursor.execute("DELETE FROM customers_temp WHERE age > 60 AND email is NULL")
    conn.commit()

    end = time.perf_counter()
    times.append(end - start)

    cursor.execute("SELECT COUNT(*) FROM customers_temp")
    customers_after = cursor.fetchall()
    print(f"Iteration {i+1} rows after delete: {customers_after[0]}")



average_time = sum(times) / len(times)

print("Run times:", times)
print("Average:", average_time)

# Cleanup
cursor.close()
conn.close()
# %%
