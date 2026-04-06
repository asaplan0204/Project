#%%
from faker import Faker
import pandas as pd
import random

fake = Faker()

#%%
# customer csv files
customers = []

# generation of data
for i in range(1, 15001):

    # special cases
    age = random.randint(18, 80)
    contact_number = fake.phone_number() if random.random() > 0.1 else None
    email = fake.email() if random.random() > 0.1 else None

    
    if age > 60 and random.random() < 0.5:
        email = None
    if age < 20 or age > 50:
        if random.random() < 0.7 and contact_number is None:
            contact_number = fake.phone_number()
    if contact_number is None and email is not None and random.random() < 0.5:
        contact_number = None

    customers.append({
        "_id": i, 
        "customer_id": i,  
        "name": fake.name(),
        "age": age,
        "contact_number": contact_number,
        "email": email
    })


# Mongodb customer csv
mongo_customers_documents = []
for c in customers:
    mongo_customers_documents.append({
        "_id": c["_id"],
        "name": c["name"],
        "age": c["age"],
        "contact_number": 'null' if c["contact_number"] is None else c["contact_number"],
        "email": 'null' if c["email"] is None else c["email"]
    })
df_mongo_customers = pd.DataFrame(mongo_customers_documents)
df_mongo_customers.to_csv("mongo_customers.csv", index=False)

# Mysql customer csv
mysql_customers_rows = []
for c in customers:
    mysql_customers_rows.append({
        "customer_id": c["customer_id"],
        "name": c["name"],
        "age": c["age"],
        "contact_number": 'NULL' if c["contact_number"] is None else c["contact_number"],
        "email": 'NULL' if c["email"] is None else c["email"]
    })
df_mysql_customers = pd.DataFrame(mysql_customers_rows)
df_mysql_customers.to_csv("mysql_customers.csv", index=False)

print(f"Generated mongo_customers.csv with {len(df_mongo_customers)} rows")
print(f"Generated mysql_customers.csv with {len(df_mysql_customers)} rows")


#%%

# sessions csv files
sessions = []
customer_ids = [c["customer_id"] for c in customers]

# generating data with special cases included
for i in range(1, 100001):
    pause_schedule = [random.randint(0, 90) for _ in range(7)]
    cust_id = random.choice(customer_ids)

    session = {
        "_id": i,
        "customer_id": cust_id,
        "watch_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
        "is_rewatch": random.choice([True, False]),
        "movie": {
            "title": fake.sentence(nb_words=2).replace(".", ""),
            "genre": random.choice(["Sci-Fi", "Drama", "Comedy", "Action", "Horror"]),
            "duration_minutes": random.randint(80, 180),
            "age_rating": random.choice(["G", "PG", "PG-13", "R"])
        },
        "moods": None if random.random() < 0.1 else random.sample(["happy", "sad", "excited", "curious", "bored", "relaxed"], 2),
        "watch_details": {
            "platform": random.choice(["Netflix", "Hulu", "Amazon Prime", "Disney+"]),
            "device": random.choice(["TV", "Laptop", "Tablet", "Phone"]),
            "pause_schedule": pause_schedule
        }
    }
    sessions.append(session)


# Mongodb session csv
mongo_documents = []
for s in sessions:
    mongo_documents.append({
        "_id": s["_id"],
        "customer_id": s["customer_id"],
        "watch_date": s["watch_date"],
        "is_rewatch": s["is_rewatch"],
        "movie": str(s["movie"]),
        "moods": 'null' if s["moods"] is None else str(s["moods"]),
        "watch_details": str(s["watch_details"])
    })
df_mongo_sessions = pd.DataFrame(mongo_documents)
df_mongo_sessions.to_csv("mongo_sessions.csv", index=False)

# Mysql session csv
mysql_rows = []
for s in sessions:
    pauses = s["watch_details"]["pause_schedule"]
    moods = s["moods"]

    mysql_rows.append({
        "customer_id": s["customer_id"],
        "watch_date": s["watch_date"],
        "is_rewatch": s["is_rewatch"],
        "movie_title": s["movie"]["title"],
        "genre": s["movie"]["genre"],
        "mood1": 'NULL' if moods is None else moods[0],
        "mood2": 'NULL' if moods is None else moods[1],
        "platform": s["watch_details"]["platform"],
        "pause_min1": pauses[0],
        "pause_min2": pauses[1],
        "pause_min3": pauses[2],
        "pause_min4": pauses[3],
        "pause_min5": pauses[4],
        "pause_min6": pauses[5],
        "pause_min7": pauses[6]
    })
df_mysql_sessions = pd.DataFrame(mysql_rows)
df_mysql_sessions.to_csv("mysql_sessions.csv", index=False)

print(f"Generated mongo_sessions.csv rows: {len(df_mongo_sessions)}")
print(f"Generated mysql_sessions.csv rows: {len(df_mysql_sessions)}")
# %%