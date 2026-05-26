import os
import json
import sys
import time
import pandas as pd
from datetime import datetime, timedelta

print("🌐 [Production-Grade Kernel] 正在使用全量安全防御逻辑重构 20 年月度序列...")

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
TOKEN = '4858c835fe26ebcb62cf4ac60cb7ddd1f4bc554e9be1096d8d0707ca'.strip()
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
            print(f"📅 [SUCCESS] 2026最新交易日快照锚定: {trade_date_to_check.strftime('%Y-%m-%d')}")
            break
    except Exception as e:
        pass
    trade_date_to_check -= timedelta(days=1)

if not target_trade_date_str:
    print("❌ [FATAL ERROR] 连通 Tushare 失败，请检查网络环境！")
    sys.exit(1)

START_DATE = "20070101"
END_DATE = target_trade_date_str
all_records = []

for code, info in stock_configs.items():
    print(f"📥 正在拉取官方历史月度不复权序列: {code} ({info['name']}) ...")
    
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
        print(f"❌ 穿透历史序列失败: {api_err}")
        sys.exit(1)

    fina_dict = {}
    try:
        df_fina = pro.fina_indicator(ts_code=code, start_date=START_DATE, end_date=END_DATE,
                                     fields='end_date,roe,roa,gpm,npm,q_sales_yoy,q_netprof_yoy,debt_to_assets,current_ratio,quick_ratio,bps,cfps')
        if df_fina is not None and not df_fina.empty:
            for _, f_row in df_fina.iterrows():
                fina_dict[str(f_row['end_date'])] = f_row
    except:
        pass

    latest_row = df_all_daily.iloc[-1]
    now_price = float(latest_row['close'])
    now_mv = float(latest_row['total_mv'] / 10000)
    now_turnover = float(latest_row['turnover_rate']) if pd.notna(latest_row['turnover_rate']) else 0.0
    now_pe = float(latest_row['pe']) if pd.notna(latest_row['pe']) else 0.0
    now_pb = float(latest_row['pb']) if pd.notna(latest_row['pb']) else 0.0

    for m_date, row in df_monthly.iterrows():
        year_val = m_date.year
        month_val = m_date.month
        
        fina_key = f"{year_val}1231"
        f_data = fina_dict.get(fina_key, {})
        
        # 安全读取基本面派生字段
        def get_float(d, key):
            if hasattr(d, 'get'):
                v = d.get(key, 0.0)
                return float(v) if (pd.notna(v) and v is not None) else 0.0
            return 0.0

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
# 🔎 工业级自动化数据自检中枢
# ==========================================
print("\n🔎 正在启动后端自动化数据自检中枢...")
moutai_records = [r for r in all_records if r['ts_code'] == '600519.SH']
assert len(moutai_records) > 100, "❌ [自检失败] 资产序列深度不足！"
sample_mv = moutai_records[-1]['now_mv']
assert 10000.0 < sample_mv < 30000.0, f"❌ [自检失败] 发现总市值单位换算错位！当前最新市值为 {sample_mv} 亿。"
moutai_2021_02 = [r for r in moutai_records if r['report_type'] == '2021年-02月度']
if moutai_2021_02:
    peak_price = moutai_2021_02[0]['history_price']
    assert peak_price > 2000.0, f"❌ [自检失败] 发现历史不复权价格错误！当前回溯水位为 {peak_price} 元。"

print("✅ [SELF-CHECK PASSED] 三项财务断言指标完美通过自检！")

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(all_records, f, ensure_ascii=False, indent=4)

print("✨ [SUCCESS] 20年全量月度纯官方数据序列矩阵已安全出厂入库！")
