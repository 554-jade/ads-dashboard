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


from sqlalchemy import create_engine, text

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

def get_db_connection():
    """Create MySQL connection using SQLAlchemy"""
    try:
        db_config = st.secrets["connections"]["mysql"]
        # Format: mysql+pymysql://user:password@host:port/database
        connection_str = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_str)
        return engine.connect()
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        st.stop()




@st.cache_data(ttl=600)
def load_data():
    """
    从 MySQL 加载 Campaign 级别数据 (t_google_cost)，并读取本地 Excel 映射表。
    """
    # 1. 加载映射表 (From Local Excel)
    manager_map = pd.DataFrame()
    
    excel_path = "Ads_BI/mapping.xlsx"
    try:
        # Load Manager Map
        # 假设 Excel 中有名为 "Manager_Map" 的 sheet，或者我们读取第一个包含 "广告账号" 的 sheet
        xls = pd.ExcelFile(excel_path)
        sheet_names = xls.sheet_names
        
        target_sheet = None
        for s in sheet_names:
            if "manager" in s.lower() or "优化师" in s:
                target_sheet = s
                break
        if not target_sheet and sheet_names:
            target_sheet = sheet_names[0] # Fallback
            
        if target_sheet:
            manager_map = pd.read_excel(xls, sheet_name=target_sheet)
            # Normalize Columns
            manager_map.columns = [c.strip() for c in manager_map.columns]
            # Ensure required columns exist
            if '广告账号' in manager_map.columns and '优化师' in manager_map.columns:
                 # Standardize ID: remove all non-digits for robust matching
                 manager_map['join_id'] = manager_map['广告账号'].astype(str).str.replace(r'\D', '', regex=True)
            else:
                 st.warning(f"映射表 {target_sheet} 缺少 '广告账号' 或 '优化师' 列")
                 manager_map = pd.DataFrame()
        
    except Exception as e:
        st.error(f"加载本地映射表失败: {e}")
    

    # 1.2 加载 Campaign -> URL 映射表 (Bridge Map)
    bridge_map = pd.DataFrame()
    try:
        # Look for sheet "广告mapping"
        if "广告mapping" in sheet_names:
            bridge_df = pd.read_excel(xls, sheet_name="广告mapping")
            bridge_df.columns = [c.strip() for c in bridge_df.columns]
            if '广告系列' in bridge_df.columns and '最终到达网址' in bridge_df.columns:
                # Create dictionary: Campaign -> URL
                # Handle duplicates: take first or last? Let's take first non-empty.
                bridge_df = bridge_df.dropna(subset=['广告系列'])
                # Clean keys
                bridge_df['广告系列'] = bridge_df['广告系列'].astype(str).str.strip()
                bridge_map = bridge_df.set_index('广告系列')['最终到达网址'].to_dict()
    except Exception as e:
         st.warning(f"加载广告映射表失败: {e}")

    # 1.3 加载 URL -> Category 映射表 (Category Map)
    category_map_dict = {}
    try:
        # Look for sheet "Category_Map"
        # Check if it exists in xls (Local) OR fetch from GSheets? 
        # User said "updated mapping this table", implying it's in the same excel file.
        cat_sheet = next((s for s in sheet_names if "category" in s.lower() or "类目" in s), None)
        
        if cat_sheet:
            cat_df = pd.read_excel(xls, sheet_name=cat_sheet)
            cat_df.columns = [c.strip() for c in cat_df.columns]
            
            def clean_url_local(url):
                if pd.isna(url) or not url: return ""
                try:
                    parsed = urlparse(str(url))
                    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
                except:
                    return ""

            if '最终到达网址' in cat_df.columns and '类目' in cat_df.columns:
                cat_df['clean_url'] = cat_df['最终到达网址'].apply(clean_url_local)
                category_map_dict = cat_df.set_index('clean_url')['类目'].to_dict()
    except Exception as e:
         st.warning(f"加载类目映射表失败: {e}")
         
    
    # 2. 加载广告数据 (From MySQL - Campaign Level)
    try:
        conn = get_db_connection()
        # t_google_cost schema: day_time, customer_id, campaign_name, cost, conversions, all_conversion_value
        query = """
            SELECT 
                day_time as '天',
                customer_id as '广告账号',
                campaign_name as '广告系列',
                cost as '费用',
                conversions as '转化数',
                conversions_value_by_conversion_date as '转化价值'
            FROM t_google_cost
            WHERE day_time >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        """
        raw_df = pd.read_sql(text(query), conn)
        conn.close()
        
        if raw_df.empty:
            return pd.DataFrame()

        # 数据类型转换与清洗
        raw_df['天'] = pd.to_datetime(raw_df['天'])
        
        numeric_cols = ['费用', '转化数', '转化价值']
        for col in numeric_cols:
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce').fillna(0)
            
        # 强制计算 ROAS
        raw_df['ROAS'] = raw_df.apply(lambda x: x['转化价值'] / x['费用'] if x['费用'] > 0 else 0, axis=1)
        
        # Standardize ID for joining
        raw_df['join_id'] = raw_df['广告账号'].astype(str).str.replace(r'\D', '', regex=True)

    except Exception as e:
        st.error(f"数据库加载失败: {e}")
        return pd.DataFrame()

    # 3. 数据合并
    
    # 3.1 优化师映射 (Manager)
    if not manager_map.empty:
        map_dedup = manager_map[['join_id', '优化师']].drop_duplicates(subset=['join_id'])
        merged_df = pd.merge(raw_df, map_dedup, on='join_id', how='left')
        merged_df['优化师'] = merged_df['优化师'].fillna("Unknown")
    else:
        merged_df = raw_df.copy()
        merged_df['优化师'] = "Unknown"

    
    # 3.2 类目映射 (Category) - VIA BRIDGE
    # Step A: Map Campaign -> URL
    def get_url_from_campaign(row):
        camp_name = str(row.get('广告系列', '')).strip()
        return bridge_map.get(camp_name, "")
    
    merged_df['最终到达网址'] = merged_df.apply(get_url_from_campaign, axis=1)
    
    # Step B: Map URL -> Category
    def get_category_from_url(url):
        if not url: return "Unknown"
        # Clean URL
        try:
            parsed = urlparse(str(url))
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
            return category_map_dict.get(clean, "Unknown")
        except:
             return "Unknown"

    merged_df['类目'] = merged_df['最终到达网址'].apply(get_category_from_url)
    
    # Fallback to inference if "Unknown"
    def infer_category_fallback(row):
        if row['类目'] != "Unknown":
            return row['类目']
        
        # Fallback to name inference
        name = str(row.get('广告系列', '')).lower()
        if 'shopping' in name: return 'Shopping'
        elif 'search' in name: return 'Search'
        elif 'pmax' in name: return 'PMax'
        elif 'brand' in name: return 'Brand'
        elif 'display' in name: return 'Display'
        elif 'youtube' in name or 'video' in name: return 'Video'
        return 'Other'

    merged_df['类目'] = merged_df.apply(infer_category_fallback, axis=1)


    


    # 3.3 补全缺失列以兼容后续逻辑
    merged_df['广告组'] = "All"
    merged_df['最终到达网址'] = ""
    merged_df['clean_url'] = ""

    # 3.4 生成 ID
    merged_df['广告组id'] = (
        merged_df['广告账号'].astype(str) + "_" +
        merged_df['类目'].astype(str) + "_" +  # Added Category to ID for uniqueness
        merged_df['广告系列'].astype(str)
    )

    return merged_df




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
