
import toml
import pandas as pd
from sqlalchemy import create_engine, text
import os

try:
    # 1. Load Secrets
    secrets_path = "Ads_BI/.streamlit/secrets.toml"
    if not os.path.exists(secrets_path):
        print(f"❌ Secrets file not found at {secrets_path}")
        exit()
        
    secrets = toml.load(secrets_path)
    db_config = secrets["connections"]["mysql"]
    
    print("✅ Loaded secrets.")
    print(f"   Host: {db_config['host']}")
    print(f"   DB: {db_config['database']}")
    print(f"   User: {db_config['username']}")

    # 2. Connect
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    print(f"   Conn String (masked): {connection_str.replace(db_config['password'], '***')}")
    
    engine = create_engine(connection_str)
    conn = engine.connect()
    print("✅ Connection verified!")

    # 3. Test Query
    query = """
        SELECT 
            day_time as '天',
            customer_id as '广告账号'
        FROM t_google_ad_cost
        LIMIT 5
    """
    print("⏳ Running test query...")
    df = pd.read_sql(text(query), conn)
    print(f"✅ Query returned {len(df)} rows.")
    print(df.head())
    
    conn.close()

except Exception as e:
    print(f"❌ Error: {e}")
