#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    start = time.perf_counter()
    doc = db.customers.aggregate([{"$lookup": {"from": "watch_sessions", "localField": "_id", "foreignField": "customer_id", "as": "sessions"}},{"$project": {"customer_id": "$_id","name": 1,"session_count": { "$size": "$sessions" }}}])
    end = time.perf_counter()
    times.append(end - start)
    
    result = next(doc, None)
    pprint(f"Iteration {i+1} first record: {result}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# %%
