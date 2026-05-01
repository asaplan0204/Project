#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    db.customers_temp.drop()
    db.customers_temp.insert_many(list(db.customers.find({})))    
    
    customers_before = db.customers_temp.count_documents({})
    pprint(f"Iteration {i+1} rows before: {customers_before}")

    start = time.perf_counter()

    db.customers_temp.delete_many({"age": {"$gt" : 60}, "email": None})

    end = time.perf_counter()
    times.append(end - start)
    
    customers_after = db.customers_temp.count_documents({})
    pprint(f"Iteration {i+1} rows after: {customers_after}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# resetting temporary collection
#%%
db.customers_temp.drop()
db.customers_temp.insert_many(list(db.customers.find({})))
customers_after = db.customers_temp.count_documents({})
pprint(f"Rows after resetting: {customers_after}")