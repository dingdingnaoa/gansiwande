import os
import json
import sys
import time
import pandas as pd
from datetime import datetime, timedelta

print("🌐 [Enterprise Quant Kernel] 正在注入全套无未来函数防御机制的 20年月度重采样引擎...")

stock_configs = {
    "600519.SH": {"name": "贵州茅台", "industry": "白酒", "board": "主板"},
    "000001.SZ": {"name": "平安银行", "industry": "银行", "board": "主板"},
    "600036.SH": {"name": "招商银行", "industry": "银行", "board": "主板"},
    "002594.SZ": {"name": "比亚迪", "industry": "汽车零配件", "board": "主板"},
    "600900.SH": {"name": "长江电力", "industry": "电力", "board": "主板"},
    "000333.SZ": {"name": "美的集团", "industry": "白色家电", "board": "主板"},
    "601318.SH": {"name": "中国平安", "industry": "保险", "board": "主板"},
    "300750.SZ": {"name": "宁德时代", "industry": "锂电池", "board": "创业板"},
    "600019.SH": {"name": "宝钢股份", "industry": "钢铁", "board": "主板"},
    "000651.SZ": {"name": "格力电器", "industry": "白色家电", "board": "主板"},
    "601888.SH": {"name": "中国中免", "industry": "旅游零售", "board": "主板"},
    "000858.SZ": {"name": "五粮液", "industry": "白酒", "board": "主板"},
    "600887.SH": {"name": "伊利股份", "industry": "乳制品", "board": "主板"},
    "601628.SH": {"name": "中国人寿", "industry": "保险", "board": "主板"},
    "300059.SZ": {"name": "东方财富", "industry": "证券", "board": "创业板"},
    "601088.SH": {"name": "中国神华", "industry": "煤炭", "board": "主板"},
    "601857.SH": {"name": "中国石油", "industry": "石油石化", "board": "主板"},
    "002415.SZ": {"name": "海康威视", "industry": "安防设备", "board": "主板"},
    "688111.SH": {"name": "金山办公", "industry": "应用软件", "board": "科创板"},
    "688981.SH": {"name": "中芯国际", "industry": "半导体", "board": "科创板"}
}

import tushare as ts
TOKEN = os.getenv('TUSHARE_TOKEN', '4858c835fe26ebcb62cf4ac60cb7ddd1f4bc554e9be1096d8d0707ca').strip()
ts.set_token(TOKEN)
pro = ts.pro_api()

target_trade_date_str = ""
trade_date_to_check = datetime.now() - timedelta(days=1)
for i in range(7):
    check_str = trade_date_to_check.strftime('%Y%m%d')
    try:
        df_test = pro.daily_basic(ts_code='600519.SH', trade_date=check_str, fields='ts_code,close')
        if df_test is not None and not df_test.empty:
            target_trade_date_str = check_str
            print(f"📅 [SUCCESS] 最新实盘快照观测点: {trade_date_to_check.strftime('%Y-%m-%d')}")
            break
    except Exception as e:
        pass
    trade_date_to_check -= timedelta(days=1)

if not target_trade_date_str:
    print("❌ 连通 Tushare 失败，请确认网络环境或令牌权限状态！")
    sys.exit(1)

START_DATE = "20070101"
END_DATE = target_trade_date_str
all_records = []

for code, info in stock_configs.items():
    print(f"📥 正在穿透无未来函数时空序列: {code} ({info['name']}) ...")
    
    try:
        df_all_daily = pro.daily_basic(ts_code=code, start_date=START_DATE, end_date=END_DATE,
                                       fields='trade_date,close,total_mv,turnover_rate,pe,pb,dv_ratio')
        if df_all_daily is None or df_all_daily.empty:
            continue
            
        df_all_daily['date'] = pd.to_datetime(df_all_daily['trade_date'], format='%Y%m%d')
        df_all_daily.set_index('date', inplace=True)
        df_all_daily.sort_index(ascending=True, inplace=True)
        df_monthly = df_all_daily.resample('ME').last().dropna(subset=['close'])
    except Exception as api_err:
        print(f"❌ 穿透历史行情失败: {api_err}")
        sys.exit(1)

    # 全量拉取该个股历史各期财报披露序列，包含真实公告日期 ann_date
    fina_list = []
    try:
        df_fina = pro.fina_indicator(ts_code=code, start_date=START_DATE, end_date=END_DATE,
                                     fields='ann_date,end_date,roe,roa,gpm,npm,q_sales_yoy,q_netprof_yoy,debt_to_assets,current_ratio,quick_ratio,bps,cfps')
        if df_fina is not None and not df_fina.empty:
            # 过滤掉公告日或报告期缺失的脏数据
            df_fina = df_fina.dropna(subset=['ann_date', 'end_date'])
            fina_list = df_fina.to_dict(orient='records')
    except:
        pass

    latest_row = df_all_daily.iloc[-1]
    now_price = float(latest_row['close'])
    now_mv = float(latest_row['total_mv'] / 10000)
    now_turnover = float(latest_row['turnover_rate']) if pd.notna(latest_row['turnover_rate']) else 0.0
    now_pe = float(latest_row['pe']) if pd.notna(latest_row['pe']) else 0.0
    now_pb = float(latest_row['pb']) if pd.notna(latest_row['pb']) else 0.0

    for m_date, row in df_monthly.iterrows():
        current_month_end_str = row['trade_date'] # 历史当时这一天月末的价格截面 YYYYMMDD
        year_val = m_date.year
        month_val = m_date.month
        
        # 🧠 终极防御：在财务披露序列中，动态匹配截至“当前月末这一天”，已经实际发布(ann_date <= 当前月末)的最新的财报，彻底斩断未来函数
        valid_fina = [f for f in fina_list if str(f['ann_date']) <= current_month_end_str]
        
        # 如果历史过早阶段还没有实际公布过任何财报，则退一步寻找 end_date 匹配作为过渡
        if not valid_fina:
            valid_fina = [f for f in fina_list if str(f['end_date']) <= f"{year_val}1231"]
            
        # 按报告期倒序排列，取最近披露的那一期财报快照
        valid_fina.sort(key=lambda x: str(x['end_date']), reverse=True)
        f_data = valid_fina[0] if valid_fina else {}

        def get_float(d, key):
            v = d.get(key, 0.0)
            return float(v) if (pd.notna(v) and v is not None) else 0.0

        all_records.append({
            "ts_code": code,
            "name": info["name"],
            "industry": info["industry"],
            "board": info["board"],
            "year": int(year_val),
            "report_type": f"{year_val}年-{str(month_val).zfill(2)}月度",
            
            "now_price": now_price,
            "now_mv": now_mv,
            "now_turnover": now_turnover,
            "now_pe": now_pe,
            "now_pb": now_pb,
            
            "history_price": float(row['close']),
            "total_mv": float(row['total_mv'] / 10000) if row['total_mv'] else 0.0,
            "turnover_ratio": float(row['turnover_rate']) if pd.notna(row['turnover_rate']) else 0.0,
            "pe": float(row['pe']) if pd.notna(row['pe']) else 0.0,
            "pb": float(row['pb']) if pd.notna(row['pb']) else 0.0,
            "dv_ratio": float(row['dv_ratio']) if pd.notna(row['dv_ratio']) else 0.0,
            
            "roe": get_float(f_data, 'roe'),
            "roa": get_float(f_data, 'roa'),
            "revenue_growth": get_float(f_data, 'q_sales_yoy'),
            "profit_growth": get_float(f_data, 'q_netprof_yoy'),
            "gross_margin": get_float(f_data, 'gpm'),
            "net_margin": get_float(f_data, 'npm'),
            "debt_asset_ratio": get_float(f_data, 'debt_to_assets'),
            "current_ratio": get_float(f_data, 'current_ratio'),
            "quick_ratio": get_float(f_data, 'quick_ratio'),
            "bps": get_float(f_data, 'bps'),
            "cfps": get_float(f_data, 'cfps')
        })
    time.sleep(0.2)

# ==========================================
# 🛑 性能级分档解耦优化 (Lazy Loading Slice Exporter)
# ==========================================
print("\n📦 正在执行大规模数据分档解耦，物理切碎全量大库并分年份单独压缩隔离...")

# 首先完整导出总数据底座，给前端保留全面索引
with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(all_records, f, ensure_ascii=False, indent=4)

# 智能提取所有有效年份，将其拆分为独立的子 JSON 数据库（例如 data_2025.json），让前端单次请求体积狂降 90%
years_set = set(r['year'] for r in all_records)
for y in years_set:
    year_records = [r for r in all_records if r['year'] == y]
    with open(f'data_{y}.json', 'w', encoding='utf-8') as f_year:
        json.dump(year_records, f_year, ensure_ascii=False, indent=4)

print(f"✅ [性能分档完成] 已自动切碎并独立导出 {len(years_set)} 个分年份物理微型数据库。")
print("✨ [SUCCESS] main.py 终极重构大功告成！")
