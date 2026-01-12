
import toml
import pandas as pd
from sqlalchemy import create_engine, text

try:
    secrets = toml.load("Ads_BI/.streamlit/secrets.toml")
    db_config = secrets["connections"]["mysql"]
    connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(connection_str)
    conn = engine.connect()

    print("--- Detailed Value Comparison (Last 30 days) ---")
    query = """
        SELECT 
            SUM(cost) as total_cost,
            SUM(all_conversion_value) as sum_all_value,
            SUM(conversions_value) as sum_conv_value,
            SUM(conversions_value_by_conversion_date) as sum_conv_date_value,
            SUM(current_model_attributed_conversion_value) as sum_attr_value
        FROM t_google_cost
        WHERE day_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    """
    df = pd.read_sql(text(query), conn)
    print(df.transpose().to_string())

    conn.close()
except Exception as e:
    print(e)
