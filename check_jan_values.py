
import toml
import pandas as pd
from sqlalchemy import create_engine, text

try:
    secrets = toml.load("Ads_BI/.streamlit/secrets.toml")
    db_config = secrets["connections"]["mysql"]
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    print("--- Value Check (Jan 1 - Jan 9) ---")
    query = """
        SELECT 
            SUM(cost) as total_cost,
            SUM(all_conversion_value) as val_all,
            SUM(conversions_value) as val_std
        FROM t_google_cost
        WHERE day_time BETWEEN '2026-01-01' AND '2026-01-09'
    """
    df = pd.read_sql(text(query), conn)
    print(df.to_string())
    
    conn.close()
except Exception as e:
    print(e)
