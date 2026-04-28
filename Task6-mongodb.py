#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    start = time.perf_counter()
    doc = db.customers.find({"contact_number" : None, "email" : {"$exists" : True,"$ne": None}})
    end = time.perf_counter()
    times.append(end - start)
    
    result = next(doc, None)
    pprint(f"Iteration {i+1} first record: {result}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# %%
