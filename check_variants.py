
import toml
import pandas as pd
from sqlalchemy import create_engine, text

try:
    secrets_path = "Ads_BI/.streamlit/secrets.toml"
    secrets = toml.load(secrets_path)
    db_config = secrets["connections"]["mysql"]
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    for t in ['t_google_variant', 't_google_product_cost', 't_fb_adset']:
        print(f"\n--- Schema for {t} ---")
        try:
             res = conn.execute(text(f"DESCRIBE {t}"))
             for row in res:
                 print(f"{row[0]}: {row[1]}")
             
             # Check distinct Count
             cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
             print(f"Row count: {cnt}")

        except Exception as e:
            print(f"Error: {e}")

    conn.close()

except Exception as e:
    print(f"Error: {e}")
