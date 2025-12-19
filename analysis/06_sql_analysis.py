import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

q1 = """
SELECT state, SUM(txn_amount) AS total_amount
FROM aggregated_transaction
GROUP BY state
ORDER BY total_amount DESC
LIMIT 10;
"""
df_q1 = pd.read_sql(q1, engine)
df_q1


q2 = """
SELECT transaction_type, SUM(txn_count) AS total_txns
FROM aggregated_transaction
GROUP BY transaction_type
ORDER BY total_txns DESC;
"""
pd.read_sql(q2, engine)


state_name = "Karnataka"

q3 = f"""
SELECT district, SUM(txn_amount) AS total_amount
FROM map_transaction
WHERE state = '{state_name}'
GROUP BY district
ORDER BY total_amount DESC
LIMIT 10;
"""
pd.read_sql(q3, engine)


q4 = """
SELECT state,
       SUM(registered_users) AS users,
       SUM(app_opens) AS app_opens
FROM map_user
GROUP BY state
ORDER BY users DESC
LIMIT 10;
"""
pd.read_sql(q4, engine)

