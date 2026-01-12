
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

    t = 't_google_keyword_cost'
    print(f"--- Schema for {t} ---")
    res = conn.execute(text(f"DESCRIBE {t}"))
    for row in res:
        print(f"{row[0]}: {row[1]}")
            
    print("\nPreview:")
    df = pd.read_sql(text(f"SELECT * FROM {t} LIMIT 3"), conn)
    print(df.to_string())

    conn.close()

except Exception as e:
    print(f"Error: {e}")
