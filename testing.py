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

    start = time.perf_counter()

    cursor.execute("DELETE FROM customers_temp WHERE name = 'Jon Mccullough'")
    conn.commit()

    end = time.perf_counter()
    times.append(end - start)

average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

cursor.close()
conn.close()


#%%
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviesdb_sql"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM customers_temp")
rows = cursor.fetchall()  # consume all results
for row in rows:
    print(row)

#%%
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="moviesdb_sql"
)
cursor = conn.cursor()
# cursor.execute("SELECT * FROM customers_temp")
# rows = cursor.fetchall()  # consume all results
# for row in rows:
#     print(row)
cursor.execute("DROP TABLE IF EXISTS customers_temp")
cursor.execute("CREATE TABLE customers_temp LIKE customers")
cursor.execute("INSERT INTO customers_temp SELECT * FROM customers")
cursor.execute("SELECT * FROM customers_temp")
rows = cursor.fetchall()  # consume all results
for row in rows:
    print(row)
conn.commit()
cursor.close()
conn.close()

# Mongodb for loop delete and update
#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    db.customer_temp.drop()
    db.customers.aggregate([{'$match': {}}, {'$out': 'customers_temp'}])

    start = time.perf_counter()

    db.customers_temp.delete_many({"name":"Jon Mccullough"})

    end = time.perf_counter()
    times.append(end - start)

average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# %%
result = list(db.customers_temp.find({}))
pprint(result)

#%%
db.customer_temp.drop()

db.customers.aggregate([{'$match': {}}, {'$out': 'customers_temp'}])
result = list(db.customers_temp.find({}))
pprint(result)
# %%
