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
    cursor.execute("DROP TABLE IF EXISTS customers_temp")
    cursor.execute("CREATE TABLE customers_temp LIKE customers")
    cursor.execute("INSERT INTO customers_temp SELECT * FROM customers")
    conn.commit()
    cursor.execute("SELECT age FROM customers_temp WHERE age > 60")
    customer_before = cursor.fetchall()
    print(f"Iteration {i+1} age before update: {customer_before[0]}")

    start = time.perf_counter()

    cursor.execute("UPDATE customers_temp SET age = age + 1 WHERE age > 60")
    conn.commit()

    end = time.perf_counter()
    times.append(end - start)

    cursor.execute("SELECT age FROM customers_temp WHERE age > 60")
    customer_after = cursor.fetchall()
    print(f"Iteration {i+1} pause_min1 before update: {customer_after[0]}")

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
cursor.execute("DROP TABLE IF EXISTS customers_temp")
cursor.execute("CREATE TABLE customers_temp LIKE customers")
cursor.execute("INSERT INTO customers_temp SELECT * FROM customers")
conn.commit()
cursor.execute("SELECT age FROM customers_temp WHERE age > 60")
customer_after = cursor.fetchall()
print(f"Resetting pause_min1 to orginal value: {customer_after[0]}")

cursor.close()
conn.close()
# %%
