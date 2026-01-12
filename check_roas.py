
import toml
import pandas as pd
from sqlalchemy import create_engine, text

try:
    secrets = toml.load("Ads_BI/.streamlit/secrets.toml")
    db_config = secrets["connections"]["mysql"]
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    print("--- Metric Comparison (Last 5 days) ---")
    query = """
        SELECT 
            SUM(cost) as total_cost,
            SUM(all_conversion_value) as sum_all_value,
            SUM(conversions_value) as sum_conv_value,
            SUM(view_through_conversions) as sum_vtc
        FROM t_google_cost
        WHERE day_time >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
    """
    df = pd.read_sql(text(query), conn)
    print(df.to_string())
    
    if df['total_cost'][0] > 0:
        roas_all = df['sum_all_value'][0] / df['total_cost'][0]
        roas_std = df['sum_conv_value'][0] / df['total_cost'][0]
        print(f"\nROAS (All Conv Value): {roas_all:.4f}")
        print(f"ROAS (Std Conv Value): {roas_std:.4f}")

    print("\n--- Check for Row Duplication (One Campaign, One Day) ---")
    query_dup = """
        SELECT *
        FROM t_google_cost
        ORDER BY day_time DESC
        LIMIT 10
    """
    df_dup = pd.read_sql(text(query_dup), conn)
    print(df_dup[['day_time', 'campaign_name', 'cost', 'all_conversion_value']].to_string())

    conn.close()
except Exception as e:
    print(e)
