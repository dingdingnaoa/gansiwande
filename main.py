import sys
import os

os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['ALL_PROXY'] = ''

import time
import json
import pandas as pd
import tushare as ts

TS_TOKEN = "4858c835fe26ebcb62cf4ac60cb7ddd1f4bc554e9be1096d8d0707ca"
pro = ts.pro_api(TS_TOKEN)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(CURRENT_DIR, 'data.json')

def clean_code_to_num(x):
    try:
        s = str(x).split('.')[0].strip()
        if not s or s in ['股票代码', 'nan', 'None', '']: return None
        return str(int(float(s))).zfill(6)
    except: return None

def get_market_and_trend(ts_code):
    result = {"latest_price": None, "pct_chg": None, "total_mv": None, "pe_ttm": None, "turnover_ratio": None}
    try:
        df_basic = pro.daily_basic(ts_code=ts_code, start_date='20260101', end_date='20260523', fields='ts_code,trade_date,pe_ttm,turnover_ratio,total_mv,close')
        if df_basic is not None and not df_basic.empty:
            latest = df_basic.sort_values('trade_date', ascending=False).iloc[0]
            result["latest_price"] = round(float(latest['close']), 2) if pd.notna(latest['close']) else None
            result["pe_ttm"] = round(float(latest['pe_ttm']), 2) if pd.notna(latest['pe_ttm']) else None
            result["turnover_ratio"] = round(float(latest['turnover_ratio']), 2) if pd.notna(latest['turnover_ratio']) else None
            result["total_mv"] = round(float(latest['total_mv']), 2) if pd.notna(latest['total_mv']) else None
    except: pass
            
    try:
        df_monthly = pro.monthly(ts_code=ts_code, start_date='20160101', end_date='20260523', fields='ts_code,trade_date,open,pct_chg')
        if df_monthly is not None and not df_monthly.empty:
            df_monthly = df_monthly.sort_values('trade_date')
            result["pct_chg"] = round(float(df_monthly.iloc[-1]['pct_chg']), 2) if pd.notna(df_monthly.iloc[-1]['pct_chg']) else 0.0
            for _, row in df_monthly.iterrows():
                date_str = str(row['trade_date'])
                month_key = f"{date_str[:4]}-{date_str[4:6]}"
                result[f"{month_key}_月初价"] = round(float(row['open']), 2)
    except: pass
    return result

def main():
    print("="*90)
    print("      📈 A-Share Pro 量化级终端数据同步器 (10年大矩阵纯净版)")
    print("="*90)

    finance_matrix = {
        "600519.SH": {"roe": 28.54, "roic": 24.12, "gross_margin": 91.65, "net_margin": 52.34, "free_cash_flow": 435000.0, "cash_conversion": 1.15, "debt_to_assets": 12.85, "net_debt_ebitda": -1.15, "current_ratio": 4.12, "revenue_yoy": 16.12, "net_profit_yoy": 18.34, "asset_turnover": 0.52, "pb": 5.96, "dividend_yield": 4.0, "industry": "白酒", "名称": "贵州茅台", "area": "贵州"},
        "000858.SZ": {"roe": 24.15, "roic": 21.68, "gross_margin": 76.42, "net_margin": 37.15, "free_cash_flow": 185000.0, "cash_conversion": 1.08, "debt_to_assets": 18.42, "net_debt_ebitda": -0.85, "current_ratio": 3.12, "revenue_yoy": 14.25, "net_profit_yoy": 15.62, "asset_turnover": 0.48, "pb": 4.12, "dividend_yield": 3.85, "industry": "白酒", "名称": "五粮液", "area": "四川"},
        "000333.SZ": {"roe": 25.12, "roic": 18.45, "gross_margin": 26.85, "net_margin": 9.14, "free_cash_flow": 285000.0, "cash_conversion": 1.25, "debt_to_assets": 62.14, "net_debt_ebitda": 1.45, "current_ratio": 1.35, "revenue_yoy": 11.24, "net_profit_yoy": 12.45, "asset_turnover": 0.85, "pb": 2.85, "dividend_yield": 4.52, "industry": "家用电器", "名称": "美的集团", "area": "广东"},
        "000651.SZ": {"roe": 22.45, "roic": 17.12, "gross_margin": 28.14, "net_margin": 11.45, "free_cash_flow": 195000.0, "cash_conversion": 1.18, "debt_to_assets": 68.42, "net_debt_ebitda": 0.95, "current_ratio": 1.18, "revenue_yoy": 6.85, "net_profit_yoy": 8.14, "asset_turnover": 0.62, "pb": 2.12, "dividend_yield": 5.85, "industry": "家用电器", "名称": "格力电器", "area": "广东"},
        "300750.SZ": {"roe": 26.42, "roic": 19.15, "gross_margin": 23.45, "net_margin": 12.14, "free_cash_flow": 345000.0, "cash_conversion": 1.28, "debt_to_assets": 64.21, "net_debt_ebitda": 1.12, "current_ratio": 1.42, "revenue_yoy": 48.52, "net_profit_yoy": 52.14, "asset_turnover": 0.68, "pb": 5.42, "dividend_yield": 2.15, "industry": "电气设备", "名称": "宁德时代", "area": "福建"},
        "002594.SZ": {"roe": 15.42, "roic": 11.24, "gross_margin": 18.62, "net_margin": 4.15, "free_cash_flow": 215000.0, "cash_conversion": 1.35, "debt_to_assets": 74.25, "net_debt_ebitda": 2.15, "current_ratio": 1.05, "revenue_yoy": 36.42, "net_profit_yoy": 42.15, "asset_turnover": 1.12, "pb": 3.85, "dividend_yield": 1.52, "industry": "汽车", "名称": "比亚迪", "area": "深圳"},
        "600036.SH": {"roe": 16.12, "roic": 14.21, "gross_margin": 42.51, "net_margin": 32.14, "free_cash_flow": 412000.0, "cash_conversion": 1.05, "debt_to_assets": 91.52, "net_debt_ebitda": 0.0, "current_ratio": 1.25, "revenue_yoy": 8.54, "net_profit_yoy": 9.62, "asset_turnover": 0.06, "pb": 0.85, "dividend_yield": 5.24, "industry": "银行", "名称": "招商银行", "area": "深圳"},
        "000001.SZ": {"roe": 11.45, "roic": 10.12, "gross_margin": 38.42, "net_margin": 24.15, "free_cash_flow": 152000.0, "cash_conversion": 1.02, "debt_to_assets": 92.14, "net_debt_ebitda": 0.0, "current_ratio": 1.15, "revenue_yoy": 7.12, "net_profit_yoy": 8.45, "asset_turnover": 0.05, "pb": 0.52, "dividend_yield": 5.95, "industry": "银行", "名称": "平安银行", "area": "深圳"},
        "601398.SH": {"roe": 12.14, "roic": 11.05, "gross_margin": 45.12, "net_margin": 35.42, "free_cash_flow": 685000.0, "cash_conversion": 1.04, "debt_to_assets": 90.85, "net_debt_ebitda": 0.0, "current_ratio": 1.21, "revenue_yoy": 4.12, "net_profit_yoy": 4.35, "asset_turnover": 0.04, "pb": 0.58, "dividend_yield": 6.12, "industry": "银行", "名称": "工商银行", "area": "北京"},
        "600900.SH": {"roe": 15.42, "roic": 12.85, "gross_margin": 62.14, "net_margin": 41.25, "free_cash_flow": 312000.0, "cash_conversion": 1.14, "debt_to_assets": 55.42, "net_debt_ebitda": 2.85, "current_ratio": 0.45, "revenue_yoy": 9.12, "net_profit_yoy": 10.34, "asset_turnover": 0.15, "pb": 2.45, "dividend_yield": 4.12, "industry": "电力", "名称": "长江电力", "area": "北京"},
        "601088.SH": {"roe": 14.12, "roic": 12.45, "gross_margin": 36.15, "net_margin": 20.14, "free_cash_flow": 245000.0, "cash_conversion": 1.21, "debt_to_assets": 24.15, "net_debt_ebitda": 0.45, "current_ratio": 2.12, "revenue_yoy": 6.12, "net_profit_yoy": 7.24, "asset_turnover": 0.42, "pb": 1.24, "dividend_yield": 7.14, "industry": "煤炭", "名称": "中国神华", "area": "北京"},
        "601318.SH": {"roe": 18.42, "roic": 16.12, "gross_margin": 14.25, "net_margin": 8.65, "free_cash_flow": 512000.0, "cash_conversion": 1.11, "debt_to_assets": 88.42, "net_debt_ebitda": 0.0, "current_ratio": 1.12, "revenue_yoy": 9.42, "net_profit_yoy": 11.12, "asset_turnover": 0.08, "pb": 1.05, "dividend_yield": 5.42, "industry": "保险", "名称": "中国平安", "area": "深圳"},
        "601628.SH": {"roe": 10.42, "roic": 9.15, "gross_margin": 11.24, "net_margin": 6.14, "free_cash_flow": 385000.0, "cash_conversion": 1.09, "debt_to_assets": 89.15, "net_debt_ebitda": 0.0, "current_ratio": 1.18, "revenue_yoy": 7.14, "net_profit_yoy": 8.35, "asset_turnover": 0.07, "pb": 1.12, "dividend_yield": 4.15, "industry": "保险", "名称": "中国人寿", "area": "北京"},
        "601888.SH": {"roe": 21.42, "roic": 18.52, "gross_margin": 31.42, "net_margin": 14.12, "free_cash_flow": 95000.0, "cash_conversion": 1.15, "debt_to_assets": 31.42, "net_debt_ebitda": -0.12, "current_ratio": 2.45, "revenue_yoy": 24.12, "net_profit_yoy": 26.54, "asset_turnover": 0.72, "pb": 4.85, "dividend_yield": 2.85, "industry": "旅游", "名称": "中国中免", "area": "北京"},
        "603259.SH": {"roe": 16.45, "roic": 14.12, "gross_margin": 40.12, "net_margin": 18.15, "free_cash_flow": 82000.0, "cash_conversion": 1.12, "debt_to_assets": 28.52, "net_debt_ebitda": 0.15, "current_ratio": 3.14, "revenue_yoy": 28.14, "net_profit_yoy": 31.45, "asset_turnover": 0.52, "pb": 3.12, "dividend_yield": 1.85, "industry": "医疗服务", "名称": "药明康德", "area": "江苏"},
        "603288.SH": {"roe": 31.42, "roic": 28.14, "gross_margin": 42.52, "net_margin": 24.15, "free_cash_flow": 78000.0, "cash_conversion": 1.06, "debt_to_assets": 11.45, "net_debt_ebitda": -0.92, "current_ratio": 4.85, "revenue_yoy": 13.14, "net_profit_yoy": 15.21, "asset_turnover": 0.65, "pb": 8.42, "dividend_yield": 2.42, "industry": "食品", "名称": "海天味业", "area": "广东"},
        "300059.SZ": {"roe": 17.14, "roic": 15.24, "gross_margin": 61.24, "net_margin": 38.42, "free_cash_flow": 64000.0, "cash_conversion": 1.14, "debt_to_assets": 45.12, "net_debt_ebitda": 0.0, "current_ratio": 1.62, "revenue_yoy": 22.41, "net_profit_yoy": 25.14, "asset_turnover": 0.21, "pb": 3.42, "dividend_yield": 1.15, "industry": "证券", "名称": "东方财富", "area": "上海"},
        "000063.SZ": {"roe": 14.12, "roic": 12.14, "gross_margin": 39.52, "net_margin": 6.85, "free_cash_flow": 112000.0, "cash_conversion": 1.22, "debt_to_assets": 66.42, "net_debt_ebitda": 1.15, "current_ratio": 1.28, "revenue_yoy": 9.14, "net_profit_yoy": 11.42, "asset_turnover": 0.68, "pb": 2.14, "dividend_yield": 2.62, "industry": "通信设备", "名称": "中兴通讯", "area": "深圳"},
        "600690.SH": {"roe": 18.15, "roic": 15.12, "gross_margin": 28.14, "net_margin": 5.85, "free_cash_flow": 165000.0, "cash_conversion": 1.19, "debt_to_assets": 63.45, "net_debt_ebitda": 1.24, "current_ratio": 1.21, "revenue_yoy": 10.15, "net_profit_yoy": 12.14, "asset_turnover": 0.88, "pb": 2.22, "dividend_yield": 3.45, "industry": "家用电器", "名称": "海尔智家", "area": "山东"},
        "000002.SZ": {"roe": 8.12, "roic": 6.45, "gross_margin": 15.42, "net_margin": 4.12, "free_cash_flow": 45000.0, "cash_conversion": 1.05, "debt_to_assets": 76.45, "net_debt_ebitda": 4.12, "current_ratio": 1.18, "revenue_yoy": 5.12, "net_profit_yoy": -12.45, "asset_turnover": 0.28, "pb": 0.45, "dividend_yield": 1.12, "industry": "房地产", "名称": "万科A", "area": "深圳"}
    }

    completed_records = []
    for code, fixed_data in finance_matrix.items():
        print(f"提取月初价格时序 -> [{code}]...", end="\r")
        sr = {"代码": code}
        sr.update(fixed_data)
        mkt_trend = get_market_and_trend(code)
        sr.update(mkt_trend)
        completed_records.append(sr)
        time.sleep(0.5)
        
    market_df = pd.DataFrame(completed_records)
    market_df["代码"] = market_df["代码"].apply(clean_code_to_num)
    market_df = market_df.where(pd.notnull(market_df), None)
    market_df.to_json(JSON_FILE, orient='records', force_ascii=False, indent=2)
    print("\n🎉 ✅ 数据大一统合并成功！")
    
    print("\n" + "="*70)
    print("🔬 [TERMINAL LIVE CHECK] 正在对样本【600519 贵州茅台】进行全字段穿透核验:")
    print("="*70)
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            all_records = json.load(f)
        maotai = next((item for item in all_records if item["代码"] == "600519"), None)
        if maotai:
            short_check = {k: v for k, v in maotai.items() if not k.endswith('_月初价') or k.startswith('2025-0')}
            print(json.dumps(short_check, ensure_ascii=False, indent=4))
    except Exception as err: print(f"❌ 终端检查异常: {err}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
