#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install mysql-connector-python sqlalchemy')


# In[13]:


import pandas as pd
from sqlalchemy import create_engine, text

engine_root = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost"
)
with engine_root.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS phonepe_pulse"))
    conn.commit()


# In[15]:


engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

engine.connect()


# In[16]:


create_table_query = """
CREATE TABLE IF NOT EXISTS aggregated_transaction (
    id INT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100),
    year INT,
    quarter INT,
    transaction_type VARCHAR(100),
    txn_count BIGINT,
    txn_amount DOUBLE
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()


# In[17]:


import pandas as pd

df = pd.read_csv(r"D:\phonepe_project\data\aggregated_transaction.csv")

df.to_sql(
    "aggregated_transaction",
    con=engine,
    if_exists="append",
    index=False
)


# In[18]:


pd.read_sql("SELECT COUNT(*) AS total_rows FROM aggregated_transaction", engine)


# In[ ]:




