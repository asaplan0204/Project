#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    start = time.perf_counter()
    result = db.customers.find_one({"$and":[{"$or":[{"age": {"$lt": 20}}, {"age": {"$gt": 50}}]}, {"$or": [{"contact_number": {"$ne": None}},{"email": {"$ne": None}}]}]})
    end = time.perf_counter()
    times.append(end - start)
    
    pprint(f"Iteration {i+1} first record: {result}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# %%
