import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import urlparse
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
import os
import datetime

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
                 # 关键：去重，防止如果映射表里同一个账号出现多次，导致合并后的数据翻倍
                 manager_map = manager_map.drop_duplicates(subset=['join_id'])
            else:
                 st.warning(f"映射表 {target_sheet} 缺少 '广告账号' 或 '优化师' 列")
                 manager_map = pd.DataFrame()
        
    except Exception as e:
        st.error(f"加载本地映射表失败: {e}")
    

    # 1.2 加载 Campaign -> URL / 落地页 / 类目 映射表 (Bridge Map)
    bridge_map = {}
    landing_page_map = {}
    category_direct_map = {}
    
    try:
        # Look for sheet "广告mapping"
        if "广告mapping" in sheet_names:
            bridge_df = pd.read_excel(xls, sheet_name="广告mapping")
            bridge_df.columns = [c.strip() for c in bridge_df.columns]
            
            # --- 强力清洗 (Deep Cleaning) ---
            if '广告系列' in bridge_df.columns:
                # 1. 处理 Excel 合并单元格 (Forward Fill)
                bridge_df['广告系列'] = bridge_df['广告系列'].ffill()
                # 2. 归一化: 转小写 + 去首尾空格 + 规范化中间空格 (把多个空格变一个)
                bridge_df['广告系列'] = bridge_df['广告系列'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip().str.lower()
            # ------------------------------

            # Campaign -> URL
            if '广告系列' in bridge_df.columns and '最终到达网址' in bridge_df.columns:
                temp_df = bridge_df.dropna(subset=['广告系列'])
                # 广告系列已在上面全局清洗过，无需重复清洗
                temp_df['最终到达网址'] = temp_df['最终到达网址'].astype(str)
                # Aggregate multiple URLs to prevent overwriting
                bridge_map = temp_df.groupby('广告系列')['最终到达网址'].apply(lambda x: ' | '.join(x.unique())).to_dict()
                
            # Campaign -> 落地页 (Landing Page)
            if '广告系列' in bridge_df.columns and '落地页' in bridge_df.columns:
                temp_df = bridge_df.dropna(subset=['广告系列'])
                # 广告系列已全局清洗
                # Ensure Landing Page is string
                temp_df['落地页'] = temp_df['落地页'].fillna("").astype(str)
                # Aggregate multiple Landing Pages
                landing_page_map = temp_df.groupby('广告系列')['落地页'].apply(lambda x: ' | '.join([v for v in x.unique() if v])).to_dict()

            # Campaign -> 类目 (Category) - DIRECT MAPPING
            if '广告系列' in bridge_df.columns and '类目' in bridge_df.columns:
                temp_df = bridge_df.dropna(subset=['广告系列'])
                # 广告系列已全局清洗
                # Ensure Category is string
                temp_df['类目'] = temp_df['类目'].fillna("Unknown").astype(str)
                # Aggregate multiple Categories (though usually 1, safety first)
                category_direct_map = temp_df.groupby('广告系列')['类目'].apply(lambda x: ' | '.join(x.unique())).to_dict()
                
    except Exception as e:
         st.warning(f"加载广告映射表失败: {e}")

    # 1.3 (Old Category Logic Removed/Disabled)
    # ...
    
    # ... (Lines 137-189 skipped for brevity, make sure to keep correct context if jump is large. 
    # Actually tool requires contiguous block or separate calls. 
    # The user wants to map "dildo" correctly. The prompt asks to fixing the "only 3 results" issue.
    # I need to update the lookup side as well.
    # To do this in one go with replace_file_content is hard if the lines are far apart (115 vs 191).
    # I will split into two edits.)
    
    # EDIT 1: Update Map Creation (Lines ~112-130)

                
    except Exception as e:
         st.warning(f"加载广告映射表失败: {e}")

    # 1.3 (Old Category Logic Removed/Disabled as per request to use '广告mapping')
    # category_map_dict = {} ...
         
    
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

    

    
    # 3.2 类目映射 (Category) - DIRECT FROM MAPPING SHEET
    # Step A: Map Campaign -> URL & Landing Page & Category
    def get_url_from_campaign(row):
        # Key normalization: Collapse multiple spaces, strip, lower to match map keys
        # "foo  bar" -> "foo bar"
        camp_name = ' '.join(str(row.get('广告系列', '')).split()).lower()
        return bridge_map.get(camp_name, "")
    
    def get_lp_from_campaign(row):
        # Key normalization
        camp_name = ' '.join(str(row.get('广告系列', '')).split()).lower()
        return landing_page_map.get(camp_name, "")
        
    def get_cat_from_campaign(row):
        # Key normalization
        camp_name = ' '.join(str(row.get('广告系列', '')).split()).lower()
        # Strictly use mapping sheet
        return category_direct_map.get(camp_name, "Unknown")

    merged_df['最终到达网址'] = merged_df.apply(get_url_from_campaign, axis=1)
    merged_df['落地页'] = merged_df.apply(get_lp_from_campaign, axis=1)
    merged_df['类目'] = merged_df.apply(get_cat_from_campaign, axis=1)


    

    # 3.3 补全缺失列以兼容后续逻辑
    merged_df['广告组'] = "All"
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
    
    categories = sorted(df_filtered_manager['类目'].astype(str).unique().tolist())
    selected_categories = st.sidebar.multiselect("类目", categories, default=categories)
    
    df_filtered_category = df_filtered_manager[df_filtered_manager['类目'].isin(selected_categories)]
    
    accounts = sorted(df_filtered_category['广告账号'].unique().tolist())
    selected_accounts = st.sidebar.multiselect("广告账号", accounts, default=accounts)
    
    final_df = df_filtered_category[df_filtered_category['广告账号'].isin(selected_accounts)]



    # -------------------------------------------------------------------------
    # Tabs
    # -------------------------------------------------------------------------
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "指挥中心", "团队与战略", "深度透视", "红黑榜 (异常诊断)", "数据仓库", "优化师目标管理"
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
            pivot_rows = st.multiselect("行维度", ['优化师', '类目', '天', '广告账号', '广告系列', '落地页'], default=['优化师'])
        with c2:
            pivot_vals = st.multiselect("数值指标", ['费用', '转化价值', 'ROAS'], default=['费用', '转化价值', 'ROAS'])
            
        if pivot_rows and pivot_vals:
            pivot_df = final_df.groupby(pivot_rows)[['费用', '转化价值']].sum().reset_index()
            pivot_df['ROAS'] = pivot_df['转化价值'] / pivot_df['费用']

            # 处理 ROAS 可能产生的无限值 (Divide by zero)
            import numpy as np
            pivot_df = pivot_df.replace([np.inf, -np.inf], 0)



            st.markdown("###### 🔽 给透视表体检 (Pivot Filters)")
            
            # 1. 维度筛选 (Dimension Filters)
            if len(pivot_rows) > 0:
                d_cols = st.columns(len(pivot_rows))
                for i, col_key in enumerate(pivot_rows):
                    with d_cols[i]:
                        # 改为文本搜索框 (Fuzzy Search)
                        search_term = st.text_input(f"🔍 {col_key}", key=f"p_filter_{col_key}", placeholder="输入关键词...")
                        if search_term:
                            # 模糊匹配：不区分大小写
                            pivot_df = pivot_df[pivot_df[col_key].astype(str).str.contains(search_term, case=False, na=False)]
            
            # 2. 数值指标筛选 (Metric Filters) - 放入折叠面板以减少干扰
            if len(pivot_vals) > 0:
                with st.expander("🔢 数值范围筛选 (Numeric Filters)", expanded=False):
                    m_cols = st.columns(len(pivot_vals))
                    for i, col_key in enumerate(pivot_vals):
                        with m_cols[i]:
                            # 标题加粗，清晰区分指标
                            st.markdown(f"**{col_key}**")
                            # 获取真实数据的边界
                            real_min = float(pivot_df[col_key].min())
                            real_max = float(pivot_df[col_key].max())
                            
                            # 默认显示逻辑：
                            # Min 默认为 0 (看起来像"无筛选")，除非真实最小值是负数
                            default_min = 0.0 if real_min >= 0 else real_min
                            # Max 默认为真实最大值
                            default_max = real_max

                            step_v = 0.01 if 'ROAS' in col_key else 100.0
                            
                            c_min, c_max = st.columns(2)
                            with c_min:
                                val_min = st.number_input("Min", value=default_min, step=step_v, key=f"min_{col_key}")
                            with c_max:
                                val_max = st.number_input("Max", value=default_max, step=step_v, key=f"max_{col_key}")
                            
                            # 应用筛选
                            pivot_df = pivot_df[(pivot_df[col_key] >= val_min) & (pivot_df[col_key] <= val_max)]

            display_cols = pivot_rows + pivot_vals
            styler = pivot_df[display_cols].style
            styler = styler.format("{:.2f}", subset=pivot_vals)
            if 'ROAS' in display_cols:
                styler = styler.background_gradient(subset=['ROAS'], cmap="RdYlGn", vmin=0.5, vmax=2.0)
            
            st.dataframe(
                styler,
                use_container_width=True,
                height=500  # 固定高度防止数据筛选时表格跳动
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

    with tab6:

        st.subheader("优化师目标管理 (Manager Goals)")
        
        # 1. 定义文件路径 (使用相对路径以适配部署环境)
        GOAL_CSV_PATH = "Ads_BI/优化师账号维度目标.csv"
        
        # 0. 日期筛选 (Date Filter)
        import datetime
        from calendar import monthrange
        col_d, _ = st.columns([1, 3])
        with col_d:
            # 用户选择日期，我们将以该日期所在的月份作为统计周期
            target_date = st.date_input("📅 选择截止日期 (默认今天)", value=datetime.date.today(), help="将统计该月1号到此日期的累计数据")
        
        # 计算月份起止
        month_start = target_date.replace(day=1)
        filter_end_date = target_date # 截止到选定的这一天
        

        
        _, days_in_month = monthrange(target_date.year, target_date.month)
        
        # 计算时间进度
        # 逻辑修改：严格按照用户选择的截止日期计算进度
        time_progress = target_date.day / days_in_month

        today = datetime.date.today()
        if target_date == today:
            status_label = "本月进行中"
        elif target_date > today:
            status_label = "未来预测"
        else:
            status_label = "历史回溯"
            
        st.info(f"🗓 统计范围: {month_start} ~ {filter_end_date} ({status_label}) | ⏳ 月时间进度: {target_date.day}/{days_in_month} = **{time_progress:.2%}**")

        # 2. 读取目标数据
        if not os.path.exists(GOAL_CSV_PATH):
            st.error(f"未找到目标文件: {GOAL_CSV_PATH}")
        else:
            try:
                # 读取 CSV，处理千分位
                goal_df = pd.read_csv(GOAL_CSV_PATH, thousands=',')
                # 清洗列名
                goal_df.columns = [c.strip() for c in goal_df.columns]
                
                # 确保数值列为浮点数
                for col in ['目标ROI', '目标GMV', '目标消耗额']:
                    if col in goal_df.columns:
                        goal_df[col] = pd.to_numeric(goal_df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                        
                # 过滤掉空行
                goal_df = goal_df.dropna(subset=['广告账号'])
                goal_df['广告账号_join'] = goal_df['广告账号'].astype(str).str.strip()

                # --- 3. 获取及筛选实际数据 (Actuals) ---
                # 使用全局 df 进行筛选
                # Note: `df` comes from global scope
                mask_month = (df['天'].dt.date >= month_start) & (df['天'].dt.date <= filter_end_date)
                month_df = df[mask_month].copy()


                
                # 聚合实际数据
                month_agg = month_df.groupby('广告账号').agg({
                    '费用': 'sum',
                    '转化价值': 'sum'
                }).reset_index()
                month_agg.rename(columns={'费用': '累计实际消耗', '转化价值': '累计GMV'}, inplace=True)
                month_agg['广告账号_join'] = month_agg['广告账号'].astype(str).str.strip()
                
                # --- 4. 合并目标与实际 ---
                # Left join goal_df on actuals
                merged = pd.merge(goal_df, month_agg[['广告账号_join', '累计实际消耗', '累计GMV']], on='广告账号_join', how='left')
                merged['累计实际消耗'] = merged['累计实际消耗'].fillna(0)
                merged['累计GMV'] = merged['累计GMV'].fillna(0)
                
                # --- 5. 计算衍生指标 ---
                # A. 月时间进度
                merged['月时间进度'] = time_progress
                
                # B. GMV 进度 = 累计GMV / 目标GMV
                merged['GMV进度'] = merged.apply(lambda x: x['累计GMV'] / x['目标GMV'] if x['目标GMV'] > 0 else 0, axis=1)
                
                # C. GMV 进度与时间进度差距
                merged['GMV进度与时间进度差距'] = merged['GMV进度'] - merged['月时间进度']
                
                # D. 消耗进度 = 累计实际消耗 / 目标消耗额
                merged['消耗进度'] = merged.apply(lambda x: x['累计实际消耗'] / x['目标消耗额'] if x['目标消耗额'] > 0 else 0, axis=1)
                
                # E. 消耗偏差值 = (累计GMV / 目标ROI) - 累计实际消耗
                # 逻辑推导：根据图片数据 (18159 / 1.9 - 9291 = 266.36 -> 267)
                # 含义：按照实际产出(GMV)和目标ROI计算出的“理论上限消耗” - “实际消耗”
                # 正值 (Green)：实际花费 < 理论上限 (省预算/高ROI)
                # 负值 (Red)：实际花费 > 理论上限 (超支/低ROI)
                merged['消耗偏差值'] = merged.apply(lambda x: (x['累计GMV'] / x['目标ROI']) - x['累计实际消耗'] if x['目标ROI'] > 0 else -x['累计实际消耗'], axis=1)
                
                # F. 消耗进度与GMV进度差 = 消耗进度 - GMV进度
                merged['消耗进度与GMV进度差'] = merged['消耗进度'] - merged['GMV进度']
                
                # G. 账号状态自动化公式
                # G. 账号状态自动化公式
                def get_status(row):
                    if row['目标消耗额'] == 0:
                        return "无计划消耗"
                    # Example logic:
                    if row['消耗进度与GMV进度差'] > 0.10: # Spend > GMV by 10%
                        return "消耗过快 (需优化)"
                    elif row['GMV进度与时间进度差距'] < -0.20:
                         return "进度严重滞后"
                    return "正常 (无需干预)"

                merged['账号状态'] = merged.apply(get_status, axis=1)

                # 6. 构造最终展示 DataFrame
                display_cols = [
                    '优化师', '广告账号', 
                    '目标ROI', '月时间进度', 
                    '目标GMV', '累计GMV', 'GMV进度', 'GMV进度与时间进度差距',
                    '目标消耗额', '累计实际消耗', '消耗进度', '消耗偏差值', 
                    '消耗进度与GMV进度差', '账号状态'
                ]
                # 重命名以便展示 '目标消耗额' -> '目标消耗'
                rename_map = {'目标消耗额': '目标消耗'}
                final_view = merged[display_cols].rename(columns=rename_map).copy()

                # --- Aggregation Row (合计) ---
                sum_row = final_view.sum(numeric_only=True)
                sum_row['优化师'] = '合计'
                sum_row['广告账号'] = ''
                sum_row['账号状态'] = ''
                sum_row['月时间进度'] = time_progress
                
                # Re-calc ratios for Total
                total_gmv = sum_row['累计GMV']
                total_goal_gmv = sum_row['目标GMV']
                total_spend = sum_row['累计实际消耗']
                total_goal_spend = sum_row['目标消耗']
                
                sum_row['GMV进度'] = total_gmv / total_goal_gmv if total_goal_gmv > 0 else 0
                sum_row['GMV进度与时间进度差距'] = sum_row['GMV进度'] - time_progress
                sum_row['消耗进度'] = total_spend / total_goal_spend if total_goal_spend > 0 else 0
                # sum_row['消耗偏差值'] 不需要重算，直接累加即可反映整体盈亏
                # sum_row['消耗偏差值'] = sum_row['累计实际消耗'] - (sum_row['目标消耗'] * time_progress) # DELETE OLD
                
                sum_row['消耗进度与GMV进度差'] = sum_row['消耗进度'] - sum_row['GMV进度']
                
                # Weighted ROI
                if total_spend > 0:
                    # ROI = Total GMV / Total Spend ?? Or Avg Target ROI?
                    # Usually "Target ROI" for Total is Goal GMV / Goal Spend
                    sum_row['目标ROI'] = total_goal_gmv / total_goal_spend if total_goal_spend > 0 else 0
                else:
                    sum_row['目标ROI'] = 0

                final_view = pd.concat([final_view, pd.DataFrame([sum_row])], ignore_index=True)
                
                # --- 7. Styling ---
                styler = final_view.style.format({
                    '目标ROI': "{:.2f}",
                    '月时间进度': "{:.2%}",
                    '目标GMV': "{:,.0f}",
                    '累计GMV': "{:,.0f}",
                    'GMV进度': "{:.2%}",
                    'GMV进度与时间进度差距': "{:.2%}",
                    '目标消耗': "{:,.0f}",
                    '累计实际消耗': "{:,.0f}",
                    '消耗进度': "{:.2%}",
                    '消耗偏差值': "{:,.0f}",
                    '消耗进度与GMV进度差': "{:.2%}"
                })
                
                # Color Logics
                def color_gmv_diff(v):
                    if pd.isna(v): return ''
                    return 'color: red; font-weight: bold' if v < 0 else ''
                
                def color_spend_gmv_diff(v):
                    if pd.isna(v): return ''
                    if v > 0: return 'color: red' # Spend faster than GMV -> Inefficient
                    if v < 0: return 'color: green'
                    return ''
                    
                def color_deviation(v):
                    if pd.isna(v): return ''
                    return 'color: red' if v < 0 else '' # Underspend logic
                
                def color_status(v):
                    return 'color: red' if v == '无计划消耗' else ''

                styler.map(color_gmv_diff, subset=['GMV进度与时间进度差距'])
                styler.map(color_spend_gmv_diff, subset=['消耗进度与GMV进度差'])
                styler.map(color_deviation, subset=['消耗偏差值'])
                styler.map(color_status, subset=['账号状态'])

                st.dataframe(styler, use_container_width=True, height=600)
                
            except Exception as e:
                st.error(f"处理目标文件出错: {e}")



if __name__ == "__main__":
    main()

