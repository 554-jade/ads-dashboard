
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

    print("--- Campaign Name Samples (t_google_cost) ---")
    df_camp = pd.read_sql(text("SELECT DISTINCT campaign_name FROM t_google_cost LIMIT 50"), conn)
    print(df_camp['campaign_name'].tolist())

    print("\n--- t_google_variant Columns ---")
    res = conn.execute(text("DESCRIBE t_google_variant"))
    cols = [row[0] for row in res]
    print(cols)
    
    # Check if variant has 'url' or 'link'
    url_cols = [c for c in cols if 'url' in c.lower() or 'link' in c.lower()]
    print(f"\nPotential URL columns in variant: {url_cols}")
    
    if url_cols:
         print("\nSample URLs from variant:")
         df_var = pd.read_sql(text(f"SELECT {url_cols[0]} FROM t_google_variant LIMIT 5"), conn)
         print(df_var)

    conn.close()

except Exception as e:
    print(f"Error: {e}")
