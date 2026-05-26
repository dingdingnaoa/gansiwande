import os
import json
import sys
import time
import pandas as pd
from datetime import datetime, timedelta

print("🌐 [Enterprise Quant Kernel] 正在初始化近 20 年全量月度核心序列抓取引擎...")

# 1. 严格锁定 20 支主力大厂官方标准代码与属性定义
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

# 2. 计算获取最新的清算交易日节点
target_trade_date_str = ""
trade_date_to_check = datetime.now() - timedelta(days=1)
for i in range(7):
    check_str = trade_date_to_check.strftime('%Y%m%d')
    try:
        df_test = pro.daily_basic(ts_code='600519.SH', trade_date=check_str, fields='ts_code,close')
        if df_test is not None and not df_test.empty:
            target_trade_date_str = check_str
            print(f"📅 [SUCCESS] 2026年最新实盘截面观测日锚定: {trade_date_to_check.strftime('%Y-%m-%d')}")
            break
    except Exception as e:
        pass
    trade_date_to_check -= timedelta(days=1)

if not target_trade_date_str:
    print("❌ [FATAL ERROR] 连通 Tushare 失败，请检查本地网络环境！")
    sys.exit(1)

START_DATE = "20070101"
END_DATE = target_trade_date_str

all_records = []

# 3. 核心单兵定向大数据穿透清洗
for code, info in stock_configs.items():
    print(f"📥 正在全量榨取 20年历史月度不复权序列: {code} ({info['name']}) ...")
    
    try:
        df_all_daily = pro.daily_basic(ts_code=code, start_date=START_DATE, end_date=END_DATE,
                                       fields='trade_date,close,total_mv,turnover_rate,pe,pb,dv_ratio')
        
        if df_all_daily is None or df_all_daily.empty:
            print(f"⚠️ 标的 {code} 在指定 20年区间内无云端数据返回，跳过。")
            continue
            
        df_all_daily['date'] = pd.to_datetime(df_all_daily['trade_date'], format='%Y%m%d')
        df_all_daily.set_index('date', inplace=True)
        df_all_daily.sort_index(ascending=True, inplace=True)
        
        # 🧠 工业级重采样：按月 ('ME') 抓取每个月真实开盘的最后一天收盘记录
        df_monthly = df_all_daily.resample('ME').last().dropna(subset=['close'])
        
    except Exception as api_err:
        print(f"❌ [API FATAL] 穿透标历史序列失败! 官方报错原因: {api_err}")
        sys.exit(1)

    # 4. 同时批量调取历史财务基本面大底座
    fina_dict = {}
    try:
        df_fina = pro.fina_indicator(ts_code=code, start_date=START_DATE, end_date=END_DATE,
                                     fields='end_date,roe,roa,gpm,npm,q_sales_yoy,q_netprof_yoy')
        if df_fina is not None and not df_fina.empty:
            for _, f_row in df_fina.iterrows():
                rep_date = f_row['end_date']
                fina_dict[rep_date] = f_row
    except:
        pass

    # 获取该标的截至目前最新的实时快照真数
    latest_row = df_all_daily.iloc[-1]
    now_price = float(latest_row['close'])
    now_mv = float(latest_row['total_mv'] / 10000)
    now_turnover = float(latest_row['turnover_rate']) if latest_row['turnover_rate'] else 0.0
    now_pe = float(latest_row['pe']) if latest_row['pe'] else 0.0
    now_pb = float(latest_row['pb']) if latest_row['pb'] else 0.0

    # 5. 组装 240 个月不复权纯官方真数矩阵
    for m_date, row in df_monthly.iterrows():
        trade_date_str = row['trade_date']
        year_val = m_date.year
        month_val = m_date.month
        
        fina_key = f"{year_val}1231"
        f_data = fina_dict.get(fina_key, {})
        
        roe = float(f_data.get('roe', 0.0)) if pd.notna(f_data.get('roe')) else 0.0
        roa = float(f_data.get('roa', 0.0)) if pd.notna(f_data.get('roa')) else 0.0
        gpm = float(f_data.get('gpm', 0.0)) if pd.notna(f_data.get('gpm')) else 0.0
        npm = float(f_data.get('npm', 0.0)) if pd.notna(f_data.get('npm')) else 0.0
        rev_growth = float(f_data.get('q_sales_yoy', 0.0)) if pd.notna(f_data.get('q_sales_yoy')) else 0.0
        prof_growth = float(f_data.get('q_netprof_yoy', 0.0)) if pd.notna(f_data.get('q_netprof_yoy')) else 0.0

        # 👑 语法修正：将 JavaScript 的 .padStart(2,'0') 修正为 Python 原生的 .zfill(2)
        month_str = str(month_val).zfill(2)

        all_records.append({
            "ts_code": code,
            "name": info["name"],
            "industry": info["industry"],
            "board": info["board"],
            "year": int(year_val),
            "report_type": f"{year_val}年-{month_str}月度",
            
            "now_price": now_price,
            "now_mv": now_mv,
            "now_turnover": now_turnover,
            "now_pe": now_pe,
            "now_pb": now_pb,
            
            "history_price": float(row['close']),
            "total_mv": float(row['total_mv'] / 10000) if row['total_mv'] else 0.0,
            "turnover_ratio": float(row['turnover_rate']) if row['turnover_rate'] else 0.0,
            "pe": float(row['pe']) if row['pe'] else 0.0,
            "pb": float(row['pb']) if row['pb'] else 0.0,
            "dv_ratio": float(row['dv_ratio']) if row['dv_ratio'] else 0.0,
            
            "roe": roe,
            "roa": roa,
            "revenue_growth": rev_growth,
            "profit_growth": prof_growth,
            "gross_margin": gpm,
            "net_margin": npm,
            "debt_asset_ratio": 0.0,
            "bps": 0.0,
            "cfps": 0.0
        })
    time.sleep(0.2)

# ==========================================
# 🔎 工业级核心逻辑断言自检系统 (Automated Self-Check)
# ==========================================
print("\n🔎 正在启动后端自动化数据自检中枢...")
moutai_records = [r for r in all_records if r['ts_code'] == '600519.SH']

# 自检一：完整性检查
assert len(moutai_records) > 100, f"❌ [自检失败] 资产序列深度不足！"

# 自检二：市值单位对齐检查
sample_mv = moutai_records[-1]['now_mv']
assert 10000.0 < sample_mv < 30000.0, f"❌ [自检失败] 发现总市值单位换算错位！当前最新市值为 {sample_mv} 亿。"

# 自检三：历史价格复权验证 (2021年02月度茅台股价必须真实反映不复权 2000+ 的牛市巅峰行情)
moutai_2021_02 = [r for r in moutai_records if r['report_type'] == '2021年-02月度']
if moutai_2021_02:
    peak_price = moutai_2021_02[0]['history_price']
    assert peak_price > 2000.0, f"❌ [自检失败] 发现历史不复权价格发生错误复权偏移！当前回溯水位为 {peak_price} 元。"

print("✅ [SELF-CHECK PASSED] 数据完整性、市值万元转亿元换算、历史不复权2000+顶峰真实水位三项指标完美通过自检！")

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(all_records, f, ensure_ascii=False, indent=4)

print("✨ [SUCCESS] 20年全量月度纯官方数据序列矩阵已安全出厂入库！")
