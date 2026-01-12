
import pandas as pd

try:
    df = pd.read_excel("/Users/yixinyue/Desktop/广告看板制作/Ads_BI/优化师账号维度目标.xlsx")
    print("Columns:", df.columns.tolist())
    print("First 5 rows:")
    print(df.head())
    print("\nData Types:")
    print(df.dtypes)
except Exception as e:
    print(f"Error reading excel: {e}")
