import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import urlparse
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re

# -----------------------------------------------------------------------------
# 配置与说明
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Antigravity Ads Cloud - 广告云",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 认证配置
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# -----------------------------------------------------------------------------
# 数据加载与处理 (ETL)
# -----------------------------------------------------------------------------

def get_gspread_client():
    """使用 Streamlit secrets 进行 Google Sheets 认证"""
    try:
        creds_dict = dict(st.secrets["connections"]["gsheets"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"认证错误: {e}")
        st.stop()

@st.cache_data(ttl=600)  # 缓存 10 分钟
def load_data():
    """
    从 Google Sheets 加载数据并执行双键映射。
    """
    client = get_gspread_client()
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    
    try:
        if spreadsheet_url.startswith("http"):
            sh = client.open_by_url(spreadsheet_url)
        else:
            sh = client.open(spreadsheet_url)
            
        def read_sheet(worksheet_name, cols=None):
            ws = sh.worksheet(worksheet_name)
            data = ws.get_all_values()
            if not data:
                return pd.DataFrame()
            
            # 去除表头前后空格
            headers = [h.strip() for h in data[0]]
            rows = data[1:]
            
            df = pd.DataFrame(rows, columns=headers)
            
            # 关键修复：去除重复列名（保留第一个）
            df = df.loc[:, ~df.columns.duplicated()]
            
            # 去除空表头列
            df = df.loc[:, df.columns != '']
            
            if cols:
                # 只有当列存在时才筛选
                existing_cols = [c for c in cols if c in df.columns]
                if existing_cols:
                    df = df[existing_cols]
            return df
            
        raw_df = read_sheet("Raw_Data")
        manager_map = read_sheet("Manager_Map")
        category_map = read_sheet("Category_Map")

        if raw_df.empty:
            return pd.DataFrame()

        # ---------------------------------------------------------------------
        # 能够识别的列名映射 (Robust Column Mapping)
        # ---------------------------------------------------------------------
        column_mapping = {
            '转化价值 (按转化时间)': '转化价值',
            '转化价值 (按转化时间) ': '转化价值', # Handle trailing space
            '转化价值（按转化时间）': '转化价值', # Chinese parenthesis
            '转化价值（按转化时间） ': '转化价值',
            'Cost': '费用',
            'Conversions': '转化数',
            'Conversion value': '转化价值'
            # Add other known aliases here
        }
        raw_df = raw_df.rename(columns=column_mapping)
        # 关键修复：重命名后可能产生重复列（例如同时有名为 A 和 B 的列，都被映射为 C），再次去重
        raw_df = raw_df.loc[:, ~raw_df.columns.duplicated()]
        # ---------------------------------------------------------------------

        # 基础清洗
        if '天' in raw_df.columns:
            raw_df['天'] = pd.to_datetime(raw_df['天'], errors='coerce')
        # 基础清洗
        if '天' in raw_df.columns:
            raw_df['天'] = pd.to_datetime(raw_df['天'], errors='coerce')
        if '广告账号' in raw_df.columns:
            # 用户反馈 Raw Data 中的 ID 可能包含后缀 (e.g. "ID | filename")，且 split('|') 可能失效
            # 改用正则提取标准的 Google Ads ID 格式 (xxx-xxx-xxxx)
            extracted_ids = raw_df['广告账号'].astype(str).str.extract(r'(\d{3}-\d{3}-\d{4})', expand=False)
            # 如果提取到了就用提取的，没提取到（可能是纯数字或其他格式）就保留原样但去除空格
            raw_df['广告账号'] = extracted_ids.fillna(raw_df['广告账号'].astype(str)).str.strip()
            
            # 关键修复：过滤掉“广告账号”为空的行 (例如 Google Sheet 的空行)
            raw_df = raw_df[raw_df['广告账号'] != '']
            # 过滤掉非法的 ID (不包含数字的，例如 'value', '--')
            raw_df = raw_df[raw_df['广告账号'].astype(str).str.contains(r'\d', regex=True)]

            # ---------------------------------------------------------------------
            # 自动去重 (Deduplication)
            # ---------------------------------------------------------------------
            # 用户需求：相同维度的数据应“覆盖”而非累加。
            # 策略：识别所有维度列（排除已知数值列），对完全相同的维度组合保留最后一行 (keep='last')。
            
            exclude_metrics = [
                '费用', '转化价值', '转化数', 'ROAS', 
                'Cost', 'Conversions', 'Conversion value', 
                'Clicks', 'Impressions', 'CTR', 'CPC', 'Views',
                'Interactions', 'Interaction rate', 'Avg. cost', 'Avg. CPM',
                'Search Impr. share', 'Display Impr. share', 'IIV', 'Invalid clicks'
            ]
            # 策略升级：不仅仅排除完全匹配的列，还要排除包含特定关键词的列 (Metric-like columns)
            # 以防止 "Avg. CPC" 或 "Ctr" 大小写/空格 差异导致去重失败
            metric_keywords = ['cost', 'value', 'cpc', 'cpm', 'ctr', 'rate', 'clicks', 'impressions', 'conversions', 'roas', 'view']
            
            def is_metric_col(col_name):
                if col_name in exclude_metrics: return True
                c_lower = col_name.lower()
                for kw in metric_keywords:
                    if kw in c_lower:
                        return True
                return False

            dedup_subset = [c for c in raw_df.columns if not is_metric_col(c)]
            
            if dedup_subset:
                # 记录去重前行数，用于 debug 或提示
                # before_count = len(raw_df)
                # 转换所有维度列为字符串并 strip，消除隐形差异
                # 注意：这只用于判断去重，不改变原始数据类型，或者我们直接改变也没关系，因为通常维度就是字符串
                # 为了安全，我们只在临时 copy 上做标准化 key
                
                # 也可以直接 inplace 清洗维度列
                for col in dedup_subset:
                     raw_df[col] = raw_df[col].astype(str).str.strip()

                raw_df = raw_df.drop_duplicates(subset=dedup_subset, keep='last')
                # after_count = len(raw_df)

            # 关键修复：去重时将“天”转为了字符串，这里必须转回 datetime，否则后续筛选会报错
            if '天' in raw_df.columns:
                raw_df['天'] = pd.to_datetime(raw_df['天'], errors='coerce')


        
        if '广告账号' in manager_map.columns:
            manager_map['广告账号'] = manager_map['广告账号'].astype(str).str.strip() # 去除 ID 空格

        # 2. 映射优化师
        if '广告账号' in raw_df.columns and '广告账号' in manager_map.columns:
            merged_df = pd.merge(raw_df, manager_map, on='广告账号', how='left')
        else:
            merged_df = raw_df.copy()
            merged_df['优化师'] = "Unknown"

        merged_df['优化师'] = merged_df.get('优化师', pd.Series(["Unknown"]*len(merged_df))).fillna("Unknown")
        
        # --- DEBUG: 映射诊断 ---
        # 如果大量 Unknown，给用户展示原因
        unknown_count = (merged_df['优化师'] == 'Unknown').sum()
        if unknown_count > 5:
            with st.expander(f"⚠️ 发现 {unknown_count} 条数据未匹配到优化师 (点击查看详情)"):
                st.write("Manager Map (前 5 行):")
                st.dataframe(manager_map.head())
                st.write("Raw Data 中未匹配的广告账号 (前 10 个):")
                unmatched_ids = merged_df[merged_df['优化师'] == 'Unknown']['广告账号'].unique()
                st.write(unmatched_ids[:10])
                st.info("请检查 'Ad Account' (Raw_Data) 和 '广告账号' (Manager_Map) 是否一致。")
        # ---------------------

        # 3. 映射类目
        def clean_url(url):
            if pd.isna(url):
                return ""
            parsed = urlparse(str(url))
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')

        if '最终到达网址' in merged_df.columns:
             merged_df['clean_url'] = merged_df['最终到达网址'].apply(clean_url)
        else:
             merged_df['clean_url'] = ""

        if '最终到达网址' in category_map.columns:
            category_map['clean_url'] = category_map['最终到达网址'].apply(clean_url)
        else:
             category_map['clean_url'] = "" # Should define clean_url column anyway
        
        if '类目' in category_map.columns:
            # 关键修复：如果你之前已经把 '类目' 写进 Raw_Data 了，这里 merge 会导致 duplicate columns (类目_x, 类目_y)
            # 所以 merge 前先 drop 掉 merged_df 里的旧类目，以 Category Map 的最新映射为准
            if '类目' in merged_df.columns:
                merged_df = merged_df.drop(columns=['类目'])
                
            category_map_dedup = category_map[['clean_url', '类目']].drop_duplicates()
            merged_df = pd.merge(merged_df, category_map_dedup, on='clean_url', how='left')
            merged_df['类目'] = merged_df['类目'].fillna("Unknown")
        else:
            # 如果 Map 里没类目，但 Raw Data 里可能有??
            if '类目' not in merged_df.columns:
                 merged_df['类目'] = "Unknown"
            
        if 'clean_url' in merged_df.columns:
            merged_df = merged_df.drop(columns=['clean_url'])
        
        numeric_cols = ['费用', '转化价值', 'ROAS', '转化数']
        # ROAS 是计算出来的，转化数是可选的。只有 费用和转化价值是必须的。
        missing_numeric = [col for col in numeric_cols if col not in merged_df.columns and col not in ['转化数', 'ROAS']]
        
        if missing_numeric:
            st.error(f"严重错误：您的 Google Sheet 'Raw_Data' 表缺少以下关键数据列: {missing_numeric}。")
            st.write(f"当前检测到的所有列名: {list(merged_df.columns)}")
            st.info("请检查您的表格表头是否有错别字、多余空格，或者列名不匹配。")
            # Return empty to prevent KeyError downstream
            return pd.DataFrame()

        for col in numeric_cols:
             if col in merged_df.columns:
                merged_df[col] = merged_df[col].astype(str).str.replace(r'[^\d\.-]', '', regex=True)
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)
        
        # 强制重新计算 ROAS，确保数据准确 (即使 Sheet 里有这列但为空)
        if '费用' in merged_df.columns and '转化价值' in merged_df.columns:
             merged_df['ROAS'] = merged_df.apply(lambda x: x['转化价值'] / x['费用'] if x['费用'] > 0 else 0, axis=1)

        return merged_df

    except Exception as e:
        st.error(f"数据加载错误: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 数据上传逻辑
# -----------------------------------------------------------------------------

def upload_data(uploaded_files):
    if not uploaded_files:
        return
    
    client = get_gspread_client()
    spreadsheet_url = st.secrets["connections"]["gsheets"]["spreadsheet"]
    
    if spreadsheet_url.startswith("http"):
        sh = client.open_by_url(spreadsheet_url)
    else:
        sh = client.open(spreadsheet_url)
    
    ws = sh.worksheet("Raw_Data")
    
    try:
        data = ws.get_all_values()
        if data:
            headers = [h.strip() for h in data[0]]
            existing_records = data[1:]
            if existing_records:
                current_df = pd.DataFrame(existing_records, columns=headers)
                # 关键修复：去除重复列名，防止 concat 报错
                current_df = current_df.loc[:, ~current_df.columns.duplicated()]
                # Filter out empty headers
                current_df = current_df.loc[:, current_df.columns != '']
            else:
                 current_df = pd.DataFrame(columns=headers)
        else:
            current_df = pd.DataFrame()
    except Exception as e:
        st.error(f"读取现有数据出错: {e}")
        current_df = pd.DataFrame()

    # Pre-cleaning existing data for merge
    if not current_df.empty:
        # Standardize '天' column for internal processing
        if '天' in current_df.columns:
            # Keep original for now, we will normalize later
             pass
        # Ensure Ad Account is string
        if '广告账号' in current_df.columns:
            current_df['广告账号'] = current_df['广告账号'].astype(str)

    # -------------------------------------------------------------------------
    # Process Uploaded Files
    # -------------------------------------------------------------------------
    
    # Load category map locally for processing new rows
    try:
        def clean_url_local(url):
            if pd.isna(url) or not url: return ""
            parsed = urlparse(str(url))
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
            
        cat_ws = sh.worksheet("Category_Map")
        cat_data = cat_ws.get_all_values()
        if cat_data:
            cat_headers = [h.strip() for h in cat_data[0]]
            cat_df = pd.DataFrame(cat_data[1:], columns=cat_headers)
            cat_df['clean_url'] = cat_df['最终到达网址'].apply(clean_url_local)
            cat_lookup = cat_df.set_index('clean_url')['类目'].to_dict()
        else:
            cat_lookup = {}
    except:
        cat_lookup = {}

    all_new_dfs = []

    for uploaded_file in uploaded_files:
        try:
            filename = uploaded_file.name
            match = re.search(r'(\d{3}-\d{3}-\d{4})', filename)
            account_id = match.group(0) if match else filename.split('.')[0].strip()
            
            df = None
            last_err = ""
            for enc in ['utf-8-sig', 'utf-16', 'utf-8', 'gbk']:
                try:
                    uploaded_file.seek(0)
                    temp_df = pd.read_csv(uploaded_file, encoding=enc, sep=None, engine='python')
                    if not temp_df.empty and len(temp_df.columns) >= 2:
                        df = temp_df
                        break
                except Exception as e:
                    last_err = str(e)
                    continue
            
            if df is None:
                st.error(f"无法读取文件 {filename}。{last_err}")
                continue
            
            df['广告账号'] = account_id
            # 兼容表头中可能存在的空格
            df.columns = [c.strip() for c in df.columns]
            # 关键修复：去除 CSV 中的重复列名
            df = df.loc[:, ~df.columns.duplicated()]

            # Date Normalization
            if '天' in df.columns:
                df['天'] = pd.to_datetime(df['天'], errors='coerce').dt.strftime('%Y-%m-%d')
                df = df.dropna(subset=['天'])
            
            # Enrich Data
            # 1. Category
            if '最终到达网址' in df.columns:
                df['类目'] = df['最终到达网址'].apply(lambda x: cat_lookup.get(clean_url_local(x), "Unknown"))
            else:
                 df['类目'] = "Unknown"

            # 2. Ad Group ID
            # Ensure columns exist with default empty string
            for col in ['广告系列', '广告组', '最终到达网址']:
                if col not in df.columns:
                    df[col] = ""
            
            df['广告组id'] = df['广告账号'].astype(str) + df['广告系列'].astype(str) + df['类目'].astype(str) + df['最终到达网址'].astype(str) + df['广告组'].astype(str)
            
            all_new_dfs.append(df)
            
        except Exception as e:
            st.error(f"处理文件 {uploaded_file.name} 时出错: {e}")

    # -------------------------------------------------------------------------
    # Merge, Dedup, and Overwrite
    # -------------------------------------------------------------------------
    if all_new_dfs:
        new_combined_df = pd.concat(all_new_dfs, ignore_index=True)
        
        # Combine Old and New
        if not current_df.empty:
            # Align schema - add missing cols to current_df if new data has them (and vice versa)
            full_df = pd.concat([current_df, new_combined_df], ignore_index=True)
        else:
            full_df = new_combined_df

        # DEFINITIVE DEDUPLICATION
        # 1. Identify Metrics (to exclude from key)
        exclude_metrics = [
            '费用', '转化价值', '转化数', 'ROAS', 
            'Cost', 'Conversions', 'Conversion value', 
            'Clicks', 'Impressions', 'CTR', 'CPC', 'Views',
            'Interactions', 'Interaction rate', 'Avg. cost', 'Avg. CPM',
            'Search Impr. share', 'Display Impr. share', 'IIV', 'Invalid clicks'
        ]
        metric_keywords = ['cost', 'value', 'cpc', 'cpm', 'ctr', 'rate', 'clicks', 'impressions', 'conversions', 'roas', 'view']
        
        def is_metric_col(col_name):
            if col_name in exclude_metrics: return True
            c_lower = col_name.lower()
            return any(kw in c_lower for kw in metric_keywords)

        # 2. Clean Dimensions for Key Generation
        dedup_subset = [c for c in full_df.columns if not is_metric_col(c)]
        
        if dedup_subset:
            # Create a temporary Normalized Key for dedup
            msg_cols = [c for c in dedup_subset if c in full_df.columns]
            
            # Helper to normalize for dedup ONLY (without changing actual data)
            # Actually, to be safe, let's normalize the ID column in the data itself
            if '广告账号' in full_df.columns:
                 # Extract standard ID format
                 full_df['广告账号'] = full_df['广告账号'].astype(str).str.extract(r'(\d{3}-\d{3}-\d{4})', expand=False).fillna(full_df['广告账号']).str.strip()
            
            for c in msg_cols:
                full_df[c] = full_df[c].astype(str).str.strip()

            before_len = len(full_df)
            full_df = full_df.drop_duplicates(subset=msg_cols, keep='last')
            after_len = len(full_df)
            st.info(f"数据合并统计: 合并前 {before_len} 行 -> 覆盖去重后 {after_len} 行 (减少 {before_len - after_len} 行)")

        # 3. Write Back to Sheet (CLEAR + UPDATE)
        # Handle NaN before writing
        full_df = full_df.fillna("")
        
        try:
            ws.clear()
            
            # 1. Update headers
            updated_headers = full_df.columns.tolist()
            ws.update(range_name='A1', values=[updated_headers])
            
            # 2. Values - Batch Upload to avoid Proxy Timeout
            updated_values = full_df.astype(str).values.tolist()
            total_rows = len(updated_values)
            chunk_size = 1000 # 每次上传 1000 行
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(0, total_rows, chunk_size):
                chunk = updated_values[i : i + chunk_size]
                # append_rows 自动处理行号，比计算 range 更稳健
                ws.append_rows(chunk)
                
                # Update progress
                progress = min((i + chunk_size) / total_rows, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"正在上传数据: {min(i + chunk_size, total_rows)} / {total_rows} 行...")
                
            status_text.empty()
            progress_bar.empty()
            
            st.success(f"数据更新成功！已覆盖写入 {total_rows} 行数据。")
            st.cache_data.clear()
            
        except Exception as e:
            st.error(f"写入 Google Sheet 失败: {e}")
            
    else:
        st.warning("没有读取到有效的新数据。")

# -----------------------------------------------------------------------------
# 主应用程序
# -----------------------------------------------------------------------------

def main():
    st.title("Antigravity Ads Cloud 🚀 - 广告云")
    
    # 1. 加载数据
    df = load_data()
    
    if df.empty:
        st.warning("未找到数据或连接失败。可能是网络波动，请尝试刷新。")
        if st.button("🔄 重试连接 (Retry)"):
            st.cache_data.clear()
            st.rerun()
        with st.sidebar:
            st.header("数据上传")
            uploaded_files = st.file_uploader("上传 CSV", accept_multiple_files=True, type="csv")
            if uploaded_files:
                if st.button("处理并上传"):
                    upload_data(uploaded_files)
        return

    # -------------------------------------------------------------------------
    # 侧边栏筛选器
    # -------------------------------------------------------------------------
    st.sidebar.header("全局筛选器")
    
    if st.sidebar.button("🔄 刷新数据 (重置缓存)"):
        st.cache_data.clear()
        st.rerun()
    
    min_date = df['天'].min().date()
    max_date = df['天'].max().date()
    start_date = st.sidebar.date_input("开始日期", value=max(min_date, max_date - pd.Timedelta(days=30)))
    end_date = st.sidebar.date_input("结束日期", value=max_date)

    mask_date = (df['天'].dt.date >= start_date) & (df['天'].dt.date <= end_date)
    df_filtered_date = df[mask_date]

    managers = ["整体"] + sorted(df_filtered_date['优化师'].unique().tolist())
    selected_managers = st.sidebar.multiselect("优化师", managers, default=["整体"])
    
    if "整体" in selected_managers:
        df_filtered_manager = df_filtered_date
    else:
        df_filtered_manager = df_filtered_date[df_filtered_date['优化师'].isin(selected_managers)]
    
    categories = sorted(df_filtered_manager['类目'].unique().tolist())
    selected_categories = st.sidebar.multiselect("类目", categories, default=categories)
    
    df_filtered_category = df_filtered_manager[df_filtered_manager['类目'].isin(selected_categories)]
    
    accounts = sorted(df_filtered_category['广告账号'].unique().tolist())
    selected_accounts = st.sidebar.multiselect("广告账号", accounts, default=accounts)
    
    final_df = df_filtered_category[df_filtered_category['广告账号'].isin(selected_accounts)]

    st.sidebar.markdown("---")
    st.sidebar.header("数据上传")
    uploaded_files = st.file_uploader("上传 CSV 文件", accept_multiple_files=True, type="csv")
    if uploaded_files:
        if st.button("处理并上传"):
            upload_data(uploaded_files)

    # -------------------------------------------------------------------------
    # Tabs
    # -------------------------------------------------------------------------
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "指挥中心", "团队与战略", "深度透视", "红黑榜 (异常诊断)", "数据仓库"
    ])

    with tab1:
        st.subheader("指挥中心 (Command Center)")
        
        current_spend = final_df['费用'].sum()
        current_val = final_df['转化价值'].sum()
        current_roas = current_val / current_spend if current_spend > 0 else 0
        
        has_conversions = '转化数' in final_df.columns
        current_conversions = final_df['转化数'].sum() if has_conversions else 1
        current_cpa = current_spend / current_conversions if has_conversions and current_conversions > 0 else 0

        days_diff = (end_date - start_date).days + 1
        prev_start = start_date - pd.Timedelta(days=days_diff)
        prev_end = start_date - pd.Timedelta(days=1)
        
        mask_prev = (df['天'].dt.date >= prev_start) & (df['天'].dt.date <= prev_end) & \
                    (df['优化师'].isin(selected_managers)) & \
                    (df['类目'].isin(selected_categories)) & \
                    (df['广告账号'].isin(selected_accounts))
        prev_df = df[mask_prev]
        
        prev_spend = prev_df['费用'].sum()
        prev_val = prev_df['转化价值'].sum()
        prev_roas = prev_val / prev_spend if prev_spend > 0 else 0
        
        permil_delta_spend = ((current_spend - prev_spend) / prev_spend) * 100 if prev_spend > 0 else 0
        permil_delta_roas = ((current_roas - prev_roas) / prev_roas) * 100 if prev_roas > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("总消耗 (Total Spend)", f"${current_spend:,.2f}", f"{permil_delta_spend:.2f}%")
        col2.metric("整体 ROAS", f"{current_roas:.2f}", f"{permil_delta_roas:.2f}%")
        col3.metric("总转化价值 (Total Value)", f"${current_val:,.2f}") 

        st.markdown("### 业绩趋势 (Performance Trend)")
        daily_trend = final_df.groupby('天').agg({'费用': 'sum', '转化价值': 'sum'}).reset_index()
        daily_trend['ROAS'] = daily_trend['转化价值'] / daily_trend['费用']
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=daily_trend['天'], y=daily_trend['费用'], name="消耗 (Spend)"),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=daily_trend['天'], y=daily_trend['ROAS'], name="ROAS", mode='lines+markers'),
            secondary_y=True,
        )
        fig.update_layout(title_text="消耗 vs ROAS 趋势")
        fig.update_yaxes(title_text="消耗 (Spend)", secondary_y=False)
        fig.update_yaxes(title_text="ROAS", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("团队与战略 (Team & Strategy)")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 人效矩阵 (People Matrix)")
            manager_perf = final_df.groupby('优化师').agg({
                '费用': 'sum', 
                '转化价值': 'sum'
            }).reset_index()
            manager_perf['ROAS'] = manager_perf['转化价值'] / manager_perf['费用']
            
            fig_bubble = px.scatter(
                manager_perf, x="费用", y="ROAS", size="转化价值", color="优化师",
                hover_name="优化师", title="优化师表现矩阵", size_max=60
            )
            fig_bubble.add_hline(y=1.0, line_dash="dash", line_color="red")
            st.plotly_chart(fig_bubble, use_container_width=True)
            
        with col2:
            st.markdown("#### 品类版图 (Category Share)")
            cat_perf = final_df.groupby('类目').agg({'费用': 'sum'}).reset_index()
            fig_pie = px.pie(cat_perf, values='费用', names='类目', title="各品类消耗占比")
            st.plotly_chart(fig_pie, use_container_width=True)

    with tab3:
        st.subheader("深度透视 (Deep Pivot)")
        c1, c2 = st.columns(2)
        with c1:
            pivot_rows = st.multiselect("行维度", ['优化师', '类目', '天', '广告账号'], default=['优化师'])
        with c2:
            pivot_vals = st.multiselect("数值指标", ['费用', '转化价值', 'ROAS'], default=['费用', '转化价值', 'ROAS'])
            
        if pivot_rows and pivot_vals:
            pivot_df = final_df.groupby(pivot_rows)[['费用', '转化价值']].sum().reset_index()
            pivot_df['ROAS'] = pivot_df['转化价值'] / pivot_df['费用']
            display_cols = pivot_rows + pivot_vals
            styler = pivot_df[display_cols].style
            if 'ROAS' in display_cols:
                styler = styler.background_gradient(subset=['ROAS'], cmap="RdYlGn", vmin=0.5, vmax=2.0)
            
            st.dataframe(
                styler,
                use_container_width=True
            )

    with tab4:
        st.subheader("红黑榜 (Red/Black List)")
        
        # 检查是否有细分维度的列
        granular_cols = ['优化师', '类目', '广告账号'] 
        if '广告组id' in final_df.columns:
            granular_cols.append('广告组id')
        if '广告系列' in final_df.columns:
            granular_cols.append('广告系列')
        if '广告组' in final_df.columns:
            granular_cols.append('广告组')
            
        # 聚合数据
        granular_perf = final_df.groupby(granular_cols).agg({'费用': 'sum', '转化价值': 'sum'}).reset_index()
        granular_perf['ROAS'] = granular_perf.apply(lambda x: x['转化价值'] / x['费用'] if x['费用'] > 0 else 0, axis=1)
        
        # 整理列顺序
        display_order = [c for c in ['优化师', '类目', '广告账号', '广告系列', '广告组', '广告组id', '费用', '转化价值', 'ROAS'] if c in granular_perf.columns]
        granular_perf = granular_perf[display_order]

        # 改为垂直排列 (Vertical Layout)
        st.markdown("### 🔴 亏损榜 (按消耗降序 - 止损优先级)")
        st.markdown("⚠️ **止损建议**: 下列广告子项消耗高且 ROAS < 1.7，应优先检查素材或关停。")
        
        # 红色列表按费用降序排（亏损最多的最先看）
        red_list = granular_perf[(granular_perf['ROAS'] < 1.7) & (granular_perf['费用'] > 0)].sort_values('费用', ascending=False).head(20)
        st.dataframe(
            red_list.style.format({"ROAS": "{:.2f}", "费用": "{:,.2f}", "转化价值": "{:,.2f}"})
                          .background_gradient(subset=['费用'], cmap="Reds"),
            use_container_width=True,
            hide_index=True
        )
        
        st.markdown("---") # Divider
        
        st.markdown("### 🌟 明星榜 (按 ROAS 降序 - 扩量优先级)")
        st.markdown("💡 **扩量建议**: 下列广告子项 ROAS > 2.0，效率极高，可在保持稳定的前提下适度扩量。")
        
        # 黑色/明星列表按 ROAS 降序排（效率最高的最先看）
        black_list = granular_perf[granular_perf['ROAS'] > 2.0].sort_values('ROAS', ascending=False).head(20)
        st.dataframe(
            black_list.style.format({"ROAS": "{:.2f}", "费用": "{:,.2f}", "转化价值": "{:,.2f}"})
                            .background_gradient(subset=['ROAS'], cmap="Greens"),
            use_container_width=True,
            hide_index=True
        )

    with tab5:
        st.subheader("数据仓库 (Data Warehouse)")
        st.dataframe(final_df)

if __name__ == "__main__":
    main()
