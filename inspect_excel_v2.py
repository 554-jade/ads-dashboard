
import pandas as pd

excel_path = "Ads_BI/mapping.xlsx"
try:
    xls = pd.ExcelFile(excel_path)
    print(f"Sheet names: {xls.sheet_names}")
    
    for sheet in xls.sheet_names:
        # Check if this is the new sheet user mentioned
        if "mapping" in sheet.lower():
            print(f"\n--- Sheet: {sheet} (First 5 rows) ---")
            df = xls.parse(sheet)
            print(df.head().to_string())
            print(f"Columns: {list(df.columns)}")
            
except Exception as e:
    print(f"Error reading excel: {e}")
