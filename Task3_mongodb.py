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
    
    customer_before = db.customers_temp.find_one({"age": {"$gt": 60}},{"age": 1, "_id": 0})
    pprint(f"Iteration {i+1} Pause schedule before: {customer_before}")

    start = time.perf_counter()

    db.customers_temp.update_many({"age": {"$gt": 60}}, {"$inc": {"age":1}})

    end = time.perf_counter()
    times.append(end - start)
    
    customer_after = db.customers_temp.find_one({"age": {"$gt": 60}},{"age": 1, "_id": 0})
    pprint(f"Iteration {i+1} Pause schedule after: {customer_after}")
average_time = sum(times) / len(times)
print("Run times:", times)
print("Average:", average_time)

# resetting pause schedule 0 to original value
#%%
db.customers_temp.drop()
db.customers_temp.insert_many(list(db.customers.find({})))
customers_after = db.customers_temp.find_one({"age": {"$gt": 60}},{"age": 1, "_id": 0})
pprint(f"Pause schedule after resetting: {customers_after}")