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
    
    session_before = db.watch_sessions_temp.find_one({"_id": 1},{"watch_details.pause_schedule": 1, "_id": 0})
    pprint(f"Iteration {i+1} Pause schedule before: {session_before}")

    start = time.perf_counter()

    db.watch_sessions_temp.update_one({"_id":1}, {"$set":{"watch_details.pause_schedule.0": 50}})

    end = time.perf_counter()
    times.append(end - start)
    
    session_after = db.watch_sessions_temp.find_one({"_id": 1},{"watch_details.pause_schedule": 1, "_id": 0})
    pprint(f"Iteration {i+1} Pause schedule after: {session_after}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# resetting pause schedule 0 to original value
#%%
db.watch_sessions_temp.drop()
db.watch_sessions_temp.insert_many(list(db.watch_sessions.find({})))
session_after = db.watch_sessions_temp.find_one({"_id": 1},{"watch_details.pause_schedule": 1, "_id": 0})
pprint(f"Pause schedule after resetting: {session_after}")
# %%
