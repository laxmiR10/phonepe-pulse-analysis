#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

BASE_PATH = r"D:\\pulse\\data\\map\\user\\hover\\country\\india\\state\\"


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

            for district, values in content["data"]["hoverData"].items():
                rows.append({
                    "state": state.replace("-", " ").title(),
                    "year": int(year),
                    "quarter": quarter,
                    "district": district,
                    "registered_users": values["registeredUsers"],
                    "app_opens": values.get("appOpens", 0)
                })

df_map_user = pd.DataFrame(rows)
df_map_user.head()


# In[6]:


engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

create_table = """
CREATE TABLE IF NOT EXISTS map_user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    district VARCHAR(100),
    registered_users BIGINT,
    app_opens BIGINT
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table))
    conn.commit()


# In[7]:


df_map_user.to_sql(
    "map_user",
    con=engine,
    if_exists="append",
    index=False
)


# In[8]:


pd.read_sql("SELECT * FROM map_user LIMIT 5", engine)


# In[ ]:




