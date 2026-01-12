
import toml
import pandas as pd
from sqlalchemy import create_engine, text

try:
    secrets = toml.load("Ads_BI/.streamlit/secrets.toml")
    db_config = secrets["connections"]["mysql"]
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    print("--- t_google_cost columns ---")
    res = conn.execute(text("DESCRIBE t_google_cost"))
    for row in res:
        print(row[0])
        
    conn.close()
except Exception as e:
    print(e)
