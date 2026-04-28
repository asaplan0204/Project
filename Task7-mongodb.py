#%%
from pymongo import MongoClient
from pprint import pprint
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["moviesdb_mongo"]

times = []

for i in range(10):
    db.watch_sessions_temp.drop()
    db.watch_sessions_temp.insert_many(list(db.watch_sessions.find({})))    
    
    watch_sessions_before = db.watch_sessions_temp.count_documents({})
    pprint(f"Iteration {i+1} rows before: {watch_sessions_before}")

    start = time.perf_counter()

    db.watch_sessions_temp.delete_many({"$or": [{"moods" : None}, {"moods" : []}, {"moods" : {"$exists" : False}}]})

    end = time.perf_counter()
    times.append(end - start)
    
    watch_sessions_after = db.watch_sessions_temp.count_documents({})
    pprint(f"Iteration {i+1} rows after: {watch_sessions_after}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# resetting temporary collection
#%%
db.watch_sessions_temp.drop()
db.watch_sessions_temp.insert_many(list(db.watch_sessions.find({})))
watch_sessions_after = db.watch_sessions_temp.count_documents({})
pprint(f"Rows after resetting: {watch_sessions_after}")