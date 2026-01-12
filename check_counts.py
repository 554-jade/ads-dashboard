
import toml
import pandas as pd
from sqlalchemy import create_engine, text
import os

try:
    secrets_path = "Ads_BI/.streamlit/secrets.toml"
    secrets = toml.load(secrets_path)
    db_config = secrets["connections"]["mysql"]
    
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    tables = ['t_google_ad_cost', 't_google_cost', 't_google_cost_20220321', 't_google_cost_20220406', 't_google_keyword_cost']
    
    for t in tables:
        try:
            res = conn.execute(text(f"SELECT COUNT(*) FROM {t}"))
            count = res.scalar()
            print(f"Table {t}: {count} rows")
            
            if count > 0:
                res = conn.execute(text(f"SELECT MAX(day_time) FROM {t}"))
                max_date = res.scalar()
                print(f"   Latest Date: {max_date}")
        except Exception as e:
            print(f"   Error checking {t}: {e}")

    conn.close()

except Exception as e:
    print(f"Error: {e}")
