
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

    query = """
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'ec-stat' 
      AND TABLE_NAME = 't_google_keyword_cost'
    """
    df = pd.read_sql(text(query), conn)
    print("Columns in t_google_keyword_cost:")
    print(df['COLUMN_NAME'].tolist())
    
    conn.close()

except Exception as e:
    print(f"Error: {e}")
