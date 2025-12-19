#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


# In[2]:


BASE_PATH = r"D:\\pulse\\data\\aggregated\\insurance\\country\\india\\state\\"


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
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            if not file.endswith(".json"):
                continue

            quarter = int(file.replace(".json", ""))

            with open(os.path.join(year_path, file), "r", encoding="utf-8") as f:
                content = json.load(f)

            data_block = content.get("data")
            if not data_block or not data_block.get("transactionData"):
                continue

            for item in data_block["transactionData"]:
                metric = item["paymentInstruments"][0]

                rows.append({
                    "state": state.replace("-", " ").title(),
                    "year": int(year),
                    "quarter": quarter,
                    "insurance_count": metric.get("count", 0),
                    "insurance_amount": metric.get("amount", 0.0)
                })

df_ins = pd.DataFrame(rows)
df_ins.head()


# In[5]:


engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

create_table = """
CREATE TABLE IF NOT EXISTS aggregated_insurance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    insurance_count BIGINT,
    insurance_amount DOUBLE
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table))
    conn.commit()


# In[6]:


df_ins.to_sql(
    "aggregated_insurance",
    con=engine,
    if_exists="append",
    index=False
)


# In[7]:


pd.read_sql(
    "SELECT * FROM aggregated_insurance LIMIT 5",
    engine
)


# In[8]:


pd.read_sql(
    "SELECT COUNT(*) FROM aggregated_insurance",
    engine
)


# In[ ]:




