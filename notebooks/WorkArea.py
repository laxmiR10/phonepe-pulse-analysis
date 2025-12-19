#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install matplotlib seaborn')


# In[2]:


states = pd.read_sql(
    "SELECT DISTINCT state FROM map_transaction",
    engine
)["state"]

available = [s for s in states if is_district_map_available(s)]
unavailable = [s for s in states if s not in available]

st.write("Available district maps:", available)
st.write("Unavailable district maps:", unavailable)


# In[ ]:




