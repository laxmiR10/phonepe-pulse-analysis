#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

BASE_PATH = r"D:\\pulse\\data\\top\\transaction\\country\\india\\state\\"


# In[2]:


state = os.listdir(BASE_PATH)[0]
year = os.listdir(os.path.join(BASE_PATH, state))[0]
file = os.listdir(os.path.join(BASE_PATH, state, year))[0]

with open(os.path.join(BASE_PATH, state, year, file), "r", encoding="utf-8") as f:
    data = json.load(f)

data["data"].keys()


# In[4]:


rows = []

for state in os.listdir(BASE_PATH):
    state_path = os.path.join(BASE_PATH, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            if not file.endswith(".json"):
                continue

            quarter = int(file.replace(".json", ""))

            with open(os.path.join(year_path, file), "r", encoding="utf-8") as f:
                content = json.load(f)

            data_block = content.get("data")
            if not data_block:
                continue

            for category in ["states", "districts", "pincodes"]:
                items = data_block.get(category)
                if not items:
                    continue

                for item in items:
                    metric = item.get("metric")

                    # metric can be dict or list
                    if isinstance(metric, list):
                        metric = metric[0] if metric else {}

                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "entity_type": category[:-1],
                        "entity_name": item.get("entityName"),
                        "txn_count": metric.get("count", 0),
                        "txn_amount": metric.get("amount", 0.0)
                    })

df_top_txn = pd.DataFrame(rows)
df_top_txn.head()


# In[5]:


df_top_txn.shape


# In[7]:


engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

create_table = """
CREATE TABLE IF NOT EXISTS top_transaction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    entity_type VARCHAR(20),
    entity_name VARCHAR(100),
    txn_count BIGINT,
    txn_amount DOUBLE
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table))
    conn.commit()


# In[8]:


df_top_txn.to_sql(
    "top_transaction",
    con=engine,
    if_exists="append",
    index=False
)


# In[9]:


pd.read_sql("SELECT * FROM top_transaction LIMIT 5", engine)


# In[10]:


BASE_PATH = r"D:\\pulse\\data\\top\\user\\country\\india\\state\\"


# In[11]:


rows = []

for state in os.listdir(BASE_PATH):
    state_path = os.path.join(BASE_PATH, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        for file in os.listdir(os.path.join(state_path, year)):
            if not file.endswith(".json"):
                continue

            quarter = int(file.replace(".json", ""))
            with open(os.path.join(state_path, year, file), "r", encoding="utf-8") as f:
                content = json.load(f)

            for category in ["districts", "pincodes"]:
                for item in content["data"][category]:
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "entity_type": category[:-1],
                        "entity_name": item["name"],
                        "registered_users": item["registeredUsers"]
                    })

df_top_user = pd.DataFrame(rows)
df_top_user.head()


# In[12]:


create_table = """
CREATE TABLE IF NOT EXISTS top_user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    entity_type VARCHAR(20),
    entity_name VARCHAR(100),
    registered_users BIGINT
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table))
    conn.commit()


# In[13]:


df_top_user.to_sql(
    "top_user",
    con=engine,
    if_exists="append",
    index=False
)


# In[14]:


pd.read_sql("SELECT * FROM top_user LIMIT 5", engine)


# In[ ]:




