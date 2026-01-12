
import pandas as pd

file_path = "/Users/yixinyue/Desktop/广告看板制作/mapping.xlsx"
try:
    xl = pd.ExcelFile(file_path)
    print(f"Sheet names: {xl.sheet_names}")
    
    for sheet in xl.sheet_names:
        print(f"\n--- Sheet: {sheet} (First 5 rows) ---")
        df = xl.parse(sheet)
        print(df.head().to_string())
        print(f"Columns: {list(df.columns)}")
        
except Exception as e:
    print(f"Error reading excel: {e}")
