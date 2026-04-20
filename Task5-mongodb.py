#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    start = time.perf_counter()
    doc = db.customers.aggregate([{"$lookup": {"from": "watch_sessions","localField": "_id","foreignField": "customer_id","as": "sessions"}},
                                  { "$unwind": "$sessions"},
                                  {"$match": {"sessions.movie.genre": "Action"}},
                                  {"$project": {"_id": 0,"customer_id": "$_id","name": 1,"age": 1,"contact_number": 1,"email": 1,"genre": "$sessions.movie.genre","session_id": "$sessions._id"}}])
    end = time.perf_counter()
    times.append(end - start)
    
    result = next(doc, None)
    pprint(f"Iteration {i+1} first record: {result}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# %%
