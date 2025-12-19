#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


# In[2]:


BASE_PATH = r"D:\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\"


# In[3]:


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
        for file in os.listdir(os.path.join(state_path, year)):
            if not file.endswith(".json"):
                continue

            quarter = int(file.replace(".json", ""))
            with open(os.path.join(state_path, year, file), "r", encoding="utf-8") as f:
                content = json.load(f)

            for district in content["data"]["hoverDataList"]:
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "year": int(year),
                    "quarter": quarter,
                    "district": district["name"],
                    "txn_count": district["metric"][0]["count"],
                    "txn_amount": district["metric"][0]["amount"]
                })

df_map_txn = pd.DataFrame(rows)
df_map_txn.head()


# In[5]:


engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

create_table = """
CREATE TABLE IF NOT EXISTS map_transaction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    district VARCHAR(100),
    txn_count BIGINT,
    txn_amount DOUBLE
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table))
    conn.commit()


# In[6]:


df_map_txn.to_sql(
    "map_transaction",
    con=engine,
    if_exists="append",
    index=False
)


# In[7]:


pd.read_sql("SELECT * FROM map_transaction LIMIT 5", engine)


# In[ ]:




