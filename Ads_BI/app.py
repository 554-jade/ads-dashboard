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
            category_map_dedup = category_map[['clean_url', '类目']].drop_duplicates()
            merged_df = pd.merge(merged_df, category_map_dedup, on='clean_url', how='left')
            merged_df['类目'] = merged_df['类目'].fillna("Unknown")
        else:
            merged_df['类目'] = "Unknown"
            
        if 'clean_url' in merged_df.columns:
            merged_df = merged_df.drop(columns=['clean_url'])
        
        numeric_cols = ['费用', '转化价值', 'ROAS', '转化数']
        missing_numeric = [col for col in numeric_cols if col not in merged_df.columns and col != '转化数'] # 转化数 is optional
        
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
                current_df = current_df.loc[:, current_df.columns != '']
            else:
                 current_df = pd.DataFrame(columns=headers)
        else:
            current_df = pd.DataFrame()
    except Exception as e:
        st.error(f"读取现有数据出错: {e}")
        current_df = pd.DataFrame()

    if not current_df.empty:
        required_cols = ['天', '广告账号']
        if not all(col in current_df.columns for col in required_cols):
             st.warning(f"警告：您的 Google Sheet 'Raw_Data' 工作表缺少必要的列头: {required_cols}。请确保表头为: 天, 广告账号, 最终到达网址, 费用, 转化价值, 转化数 (可选)")
             # 虽然缺少列，但如果用户确认要传，我们可以尝试。但去重会失效。
             # 为了安全，这里我们仍然允许上传，但去重逻辑会被跳过。
             existing_keys = set()
        else:
            current_df['天'] = pd.to_datetime(current_df['天'], errors='coerce')
            current_df['广告账号'] = current_df['广告账号'].astype(str)
            existing_keys = set(zip(current_df['广告账号'], current_df['天']))
    else:
        existing_keys = set()
        # 如果当前 sheet 为空，我们需要这一步的 headers 吗？
        # 如果 data 为空，ws.get_all_values() 返回 []，所以 headers 未定义。
        # 我们需要在 new_rows_list 生成后，根据 new_rows key 来作为 header 初始化 sheet。
        pass

    # 加载类目映射以生成 广告组id
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

    new_rows_list = []
    
    for uploaded_file in uploaded_files:
        try:
            filename = uploaded_file.name
            # 使用正则提取账号 ID (xxx-xxx-xxxx)
            match = re.search(r'(\d{3}-\d{3}-\d{4})', filename)
            account_id = match.group(0) if match else filename.split('.')[0].strip()
            
            # 尝试多种编码和分隔符读取 CSV
            df = None
            last_err = ""
            for enc in ['utf-8-sig', 'utf-16', 'utf-8', 'gbk']:
                try:
                    uploaded_file.seek(0)
                    # 使用 sep=None 和 engine='python' 自动检测分隔符 (如逗号、制表符)
                    temp_df = pd.read_csv(uploaded_file, encoding=enc, sep=None, engine='python')
                    if not temp_df.empty and len(temp_df.columns) >= 2:
                        df = temp_df
                        break
                except Exception as e:
                    last_err = str(e)
                    continue
            
            if df is None:
                st.error(f"无法读取文件 {filename}。错误详情: {last_err}")
                st.info("建议：请尝试在 Excel 中打开该文件，检查内容是否正常，并另存为标准的 CSV (逗号分隔) 格式后重新上传。")
                continue
            
            df['广告账号'] = account_id
            # 兼容表头中可能存在的空格
            df.columns = [c.strip() for c in df.columns]
            
            df['天'] = pd.to_datetime(df['天'], errors='coerce')
            df = df.dropna(subset=['天']) # 过滤掉日期解析失败的行
            
            for _, row in df.iterrows():
                key = (str(row['广告账号']), row['天'])
                if key not in existing_keys:
                    row_dict = row.to_dict()
                    row_dict['天'] = row['天'].strftime('%Y-%m-%d')
                    row_dict['广告账号'] = str(row_dict['广告账号'])
                    
                    # 匹配类目
                    url_val = str(row_dict.get('最终到达网址', ''))
                    clean_u = clean_url_local(url_val)
                    category_found = cat_lookup.get(clean_u, "Unknown")
                    row_dict['类目'] = category_found
                    
                    # 生成广告组id: 广告账号 + 广告系列 + 类目 + 落地页 + 广告组
                    acc = str(row_dict.get('广告账号', ''))
                    cmp = str(row_dict.get('广告系列', ''))
                    cat = str(row_dict.get('类目', ''))
                    grp = str(row_dict.get('广告组', ''))
                    
                    row_dict['广告组id'] = f"{acc}{cmp}{cat}{url_val}{grp}"
                    
                    new_rows_list.append(row_dict)
                    
        except Exception as e:
            st.error(f"处理文件 {uploaded_file.name} 时出错: {e}")
    
    if new_rows_list:
        # 获取最新的 headers (如果 sheet 不为空)
        if not current_df.empty and 'headers' in locals():
            target_headers = headers
        elif 'headers' in locals() and headers: # sheet 有头但无数据
             target_headers = headers
        else:
            # Sheet 是完全空的，用新数据的 keys 作为 headers
            target_headers = list(new_rows_list[0].keys())
            # 确保关键列在前面? 可选。先这样。
            ws.append_row(target_headers) # 先写入表头
        
        rows_to_append = []
        for item in new_rows_list:
            row_data = []
            for col in target_headers:
                val = item.get(col, "")
                if pd.isna(val):
                     val = ""
                row_data.append(val)
            rows_to_append.append(row_data)
            
        ws.append_rows(rows_to_append)
        
        st.success(f"成功添加 {len(new_rows_list)} 行数据！")
        st.cache_data.clear()
    else:
        st.warning("没有新数据可添加（检测到重复数据）。")

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
            st.dataframe(
                pivot_df[display_cols].style.background_gradient(subset=['ROAS'], cmap="RdYlGn", vmin=0.5, vmax=2.0),
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
        st.markdown("⚠️ **止损建议**: 下列广告子项消耗高且 ROAS < 1.0，应优先检查素材或关停。")
        
        # 红色列表按费用降序排（亏损最多的最先看）
        red_list = granular_perf[(granular_perf['ROAS'] < 1.0) & (granular_perf['费用'] > 0)].sort_values('费用', ascending=False).head(20)
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
