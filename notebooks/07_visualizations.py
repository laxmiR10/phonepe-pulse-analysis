#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)


# In[3]:


df = pd.read_sql("""
SELECT state, SUM(txn_amount) AS total_amount
FROM aggregated_transaction
GROUP BY state
ORDER BY total_amount DESC
LIMIT 10;
""", engine)

plt.figure()
plt.bar(df["state"], df["total_amount"])
plt.xticks(rotation=45, ha="right")
plt.title("Top 10 States by Transaction Amount")
plt.tight_layout()
plt.show()


# In[4]:


df2 = pd.read_sql("""
SELECT transaction_type, SUM(txn_count) AS total_txns
FROM aggregated_transaction
GROUP BY transaction_type;
""", engine)

plt.figure()
plt.pie(df2["total_txns"], labels=df2["transaction_type"], autopct="%1.1f%%")
plt.title("Payment Category Distribution")
plt.show()


# In[5]:


df3 = pd.read_sql("""
SELECT state,
       SUM(registered_users) AS users,
       SUM(app_opens) AS app_opens
FROM map_user
GROUP BY state
ORDER BY users DESC
LIMIT 10;
""", engine)

plt.figure()
plt.bar(df3["state"], df3["users"], label="Registered Users")
plt.bar(df3["state"], df3["app_opens"], bottom=df3["users"], label="App Opens")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.title("User Engagement by State")
plt.tight_layout()
plt.show()


# In[ ]:




