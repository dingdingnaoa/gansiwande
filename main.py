import sys
import os
import time
import requests
import pandas as pd
import io
import random
import datetime
from datetime import datetime as dt
import traceback
import json

# ================= ⚙️ 用户配置 (Web部署版) =================

# 1. 锁定脚本所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. 文件路径配置
# 注意：Excel在云端不一定需要，但为了调试可以保留
EXCEL_NAME = os.path.join(CURRENT_DIR, "market_data.xlsx")
FINANCIAL_FILE = os.path.join(CURRENT_DIR, 'temp_data_financial.csv')
PRICE_FILE = os.path.join(CURRENT_DIR, 'temp_price_history.csv')
JSON_FILE = os.path.join(CURRENT_DIR, 'data.json') # 【新增】Web数据源

# 爬虫伪装
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15"
]

INDICATOR_MAPPING = {
    '基本每股收益': 'EPS', '每股净资产': 'BVPS', '每股经营活动': 'OCFPS',
    '净资产收益率': 'ROE', '销售净利率': '净利率', '销售毛利率': '毛利率',
    '营业总收入': '营收', '净利润': '净利', '扣非净利润': '扣非净利',
    '资产负债率': '负债率', '流动比率': '流动比', '速动比率': '速动比',
    '存货周转率': '存货周转', '应收账款周转率': '应收周转'
}

META_INFO = {
    "代码": "文本", "名称": "文本", "最新价": "元", "涨跌幅%": "%",
    "总市值(万)": "万元", "市盈率(动)": "倍", "市净率": "倍", "换手率%": "%", "成交额(万)": "万元",
    "EPS": "元", "BVPS": "元", "OCFPS": "元", "ROE": "%", "净利率": "%", "毛利率": "%",
    "营收": "元", "净利": "元", "扣非净利": "元", "负债率": "%", "流动比": "倍", "速动比": "倍",
    "存货周转": "次", "应收周转": "次"
}

os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

# ================= 🛠️ 工具函数 =================

def clean_code(x):
    try:
        s = str(x).strip()
        if not s or s == '股票代码' or s.lower() == 'nan' or '代码' in s: return None
        return str(int(float(s))).zfill(6)
    except:
        return s.zfill(6) if s else None

def get_random_header():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://finance.sina.com.cn/"
    }

def to_wan(x):
    if x == '-' or x is None: return '-'
    try: return round(float(x) / 10000, 2)
    except: return x

def get_sina_symbol(code):
    if code.startswith('6'): return f"sh{code}"
    if code.startswith('0') or code.startswith('3'): return f"sz{code}"
    if code.startswith('8') or code.startswith('4'): return f"bj{code}"
    return f"sz{code}" 

# ================= 阶段一：行情 + 月均价 =================

def fetch_market_snapshot():
    print(f"\n🚀 [阶段一] 拉取全市场实时行情...")
    all_dfs = []
    page = 1
    # 为了演示速度，如果是在GitHub Actions里，可以适当增加并发或页数
    # 这里保持稳健的单线程
    while page <= 100: 
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        params = {"page": str(page), "num": "80", "sort": "changepercent", "asc": "0", "node": "hs_a", "symbol": "", "_s_r_a": "sort"}
        try:
            res = requests.get(url, params=params, headers=get_random_header(), timeout=10)
            if not res.text or res.text == 'null' or res.text == '[]': break
            df = pd.read_json(io.StringIO(res.text), dtype={'code': str})
            if not df.empty: all_dfs.append(df)
            else: break
        except: pass
        page += 1
        time.sleep(0.05)
        
    if not all_dfs: return pd.DataFrame()
    
    full_df = pd.concat(all_dfs, ignore_index=True)
    rename_map = {
        "code": "代码", "name": "名称", "trade": "最新价", "changepercent": "涨跌幅%", 
        "mktcap": "总市值(万)", "per": "市盈率(动)", "pb": "市净率", "turnoverratio": "换手率%", "amount": "成交额"
    }
    cols = [c for c in rename_map.keys() if c in full_df.columns]
    df_final = full_df[cols].rename(columns=rename_map)
    df_final["代码"] = df_final["代码"].apply(clean_code)
    df_final = df_final.dropna(subset=['代码'])
    
    if "成交额" in df_final.columns:
        df_final["成交额(万)"] = df_final["成交额"].apply(to_wan)
        del df_final["成交额"]
    if "总市值(万)" in df_final.columns:
        df_final["总市值(万)"] = df_final["总市值(万)"].apply(lambda x: round(float(x), 2) if x else '-')

    print(f"   ✅ 获取到 {len(df_final)} 只股票基础行情")
    return df_final

def get_stock_monthly_history(code):
    symbol = get_sina_symbol(code)
    url = f"https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=400"
    try:
        res = requests.get(url, headers=get_random_header(), timeout=5)
        data = res.json()
        if not data: return None
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df['close'] = df['close'].astype(float)
        df.set_index('day', inplace=True)
        # 按月计算均价
        monthly_df = df['close'].resample('ME').mean().sort_index(ascending=False)
        last_12 = monthly_df.head(12)
        result = {}
        for date, price in last_12.items():
            col_name = f"{date.strftime('%Y-%m')}_均价"
            result[col_name] = round(price, 2)
        return result
    except: return None

def augment_with_monthly_prices(market_df):
    print(f"\n📊 [阶段一·补充] 正在计算/读取月度均价...")
    cached_prices = pd.DataFrame()
    
    # 读取缓存 (GitHub Action pull下来的文件)
    if os.path.exists(PRICE_FILE):
        try:
            cached_prices = pd.read_csv(PRICE_FILE, dtype={'代码': str})
            cached_prices['代码'] = cached_prices['代码'].apply(clean_code)
            cached_prices = cached_prices.set_index('代码')
            print(f"   📂 成功加载月价缓存: {len(cached_prices)} 条")
        except: pass
    
    target_codes = market_df['代码'].tolist()
    # 找出缓存里没有的股票
    todo_codes = [c for c in target_codes if c not in cached_prices.index]
    
    print(f"   需补录: {len(todo_codes)} 只")

    new_data_list = []
    if todo_codes:
        # 为了避免云端运行超时，限制每次最多补录 500 个 (每天跑一点，慢慢就全了)
        # 第一次运行会比较久
        limit = 2000 
        print(f"   ⏳ 本次运行限制补录 {limit} 只，防止超时...")
        
        for i, code in enumerate(todo_codes[:limit]):
            if i % 50 == 0: print(f"   进度: {i}/{len(todo_codes[:limit])}...", end="\r")
            monthly_data = get_stock_monthly_history(code)
            if monthly_data:
                monthly_data['代码'] = code
                new_data_list.append(monthly_data)
            time.sleep(0.02)

    if new_data_list:
        new_df = pd.DataFrame(new_data_list)
        new_df.set_index('代码', inplace=True)
        if not cached_prices.empty:
            final_cache = pd.concat([cached_prices, new_df])
            final_cache = final_cache[~final_cache.index.duplicated(keep='last')]
        else:
            final_cache = new_df
        final_cache.to_csv(PRICE_FILE, encoding='utf-8-sig')
        cached_prices = final_cache
        print(f"   ✅ 月价缓存已更新并保存。")

    market_df = market_df.set_index('代码')
    cached_prices = cached_prices.reindex(market_df.index).dropna(how='all')
    market_df = market_df.join(cached_prices)
    market_df = market_df.reset_index()
    return market_df

# ================= 阶段二：财务数据补录 =================

def get_existing_financial_codes():
    if not os.path.exists(FINANCIAL_FILE): return set()
    try:
        df = pd.read_csv(FINANCIAL_FILE, dtype=str, on_bad_lines='skip')
        col = '股票代码' if '股票代码' in df.columns else df.columns[1]
        codes = df[col].apply(clean_code).dropna()
        return set(codes.unique())
    except: return set()

def fetch_financial_metrics(code):
    url = f"https://money.finance.sina.com.cn/corp/go.php/vFD_FinancialGuideLine/stockid/{code}/displaytype/4.phtml"
    try:
        response = requests.get(url, headers=get_random_header(), timeout=8)
        response.encoding = 'gb18030'
        if len(response.text) < 800: return None
        final_rows = []
        tables = pd.read_html(io.StringIO(response.text), header=None)
        for df in tables:
            if df.shape[1] < 2: continue
            if df.iloc[:, 0].astype(str).str.contains('每股收益|净资产收益率', na=False).any():
                df = df.set_index(df.columns[0])
                df.index = df.index.astype(str).str.strip()
                raw_dates = df.iloc[0].astype(str)
                if raw_dates.str.contains('-|20', na=False).any():
                    df.columns = raw_dates
                    df = df.iloc[1:]
                df = df.loc[:, df.columns.notna()]
                cols = sorted(df.columns, key=lambda x: str(x), reverse=True)
                df = df[cols].iloc[:, :8]
                all_indices = df.index.astype(str)
                for keyword, short_name in INDICATOR_MAPPING.items():
                    candidates = all_indices[all_indices.str.contains(keyword, na=False)]
                    clean_candidates = [c for c in candidates if '增长率' not in c and '同比' not in c]
                    best_match = clean_candidates[0] if clean_candidates else None
                    if best_match:
                        row = df.loc[best_match].copy()
                        row.name = short_name
                        final_rows.append(row)
                break
        if not final_rows: return None
        result_df = pd.DataFrame(final_rows)
        result_df.insert(0, '股票代码', code)
        result_df.index.name = '指标'
        result_df = result_df.reset_index()
        return result_df
    except: pass
    return None

def run_financial_crawler(target_codes):
    print(f"\n🚀 [阶段二] 财务数据智能补录...")
    done_codes = get_existing_financial_codes()
    target_codes_clean = [clean_code(c) for c in target_codes if clean_code(c)]
    todo_codes = [c for c in target_codes_clean if c not in done_codes]
    
    print(f"   已缓存: {len(done_codes)}, 需补录: {len(todo_codes)}")
    
    # 同样限制每次运行的补录数量，防止GitHub Action超时（通常限制6小时，但最好控制在30分钟内）
    limit = 200 
    if len(todo_codes) > limit:
        print(f"   ⚠️ 剩余任务较多，本次只处理前 {limit} 个，留给下次自动运行...")
        todo_codes = todo_codes[:limit]

    if not todo_codes:
        print("   ✅ 财务数据已最新。")
        return

    buffer = []
    try:
        for i, code in enumerate(todo_codes):
            print(f"   [{i+1}/{len(todo_codes)}] 财务: {code} ... ", end="", flush=True)
            try:
                df = fetch_financial_metrics(code)
                if df is not None:
                    buffer.append(df)
                    print("√")
                else:
                    print("x")
            except: print("x")
            
            if len(buffer) >= 5:
                pd.concat(buffer, ignore_index=True).to_csv(FINANCIAL_FILE, mode='a', index=False, header=not os.path.exists(FINANCIAL_FILE), encoding='utf-8-sig')
                buffer = []
            time.sleep(1.0)
            
        if buffer: 
            pd.concat(buffer, ignore_index=True).to_csv(FINANCIAL_FILE, mode='a', index=False, header=not os.path.exists(FINANCIAL_FILE), encoding='utf-8-sig')
    except KeyboardInterrupt: pass

# ================= 阶段三：Web数据生成 =================

def merge_and_export(market_df):
    print(f"\n🧩 [阶段三] 生成 Web 数据 (JSON)...")
    
    try:
        fin_df = pd.read_csv(FINANCIAL_FILE, dtype=str, on_bad_lines='skip')
        fin_df['股票代码'] = fin_df['股票代码'].apply(clean_code)
        fin_df = fin_df.drop_duplicates(subset=['股票代码', '指标'], keep='last')
        
        for col in fin_df.columns:
            if col not in ['股票代码', '指标']:
                fin_df[col] = pd.to_numeric(fin_df[col], errors='ignore')
                
        id_vars = [c for c in fin_df.columns if '指标' in c or '代码' in c]
        date_cols = [c for c in fin_df.columns if c not in id_vars]
        melted = fin_df.melt(id_vars=id_vars, value_vars=date_cols, var_name='日期', value_name='数值')
        melted = melted.dropna(subset=['数值'])
        
        indicator_col = next((c for c in id_vars if '指标' in c), None)
        pivot_df = melted.pivot_table(index='股票代码', columns=['日期', indicator_col], values='数值', aggfunc='first')
        
        # 排序
        sorted_cols = sorted(pivot_df.columns, key=lambda x: str(x[0]), reverse=True)
        pivot_df = pivot_df[sorted_cols]
        
    except Exception as e:
        print(f"   ⚠️ 财务数据异常: {e}")
        pivot_df = pd.DataFrame()

    # 【Web适配核心】：扁平化列名
    if isinstance(pivot_df.columns, pd.MultiIndex):
        # 将 ('2023-12-31', 'EPS') 变成 '2023-12-31_EPS'
        pivot_df.columns = [f"{col[0]}_{col[1]}" for col in pivot_df.columns]

    market_df['代码'] = market_df['代码'].apply(clean_code)
    market_df = market_df.set_index('代码')
    
    # Join
    final_df = market_df.join(pivot_df, how='left')
    final_df = final_df.reset_index()

    # 替换 NaN 为 None (JSON标准)
    final_df = final_df.where(pd.notnull(final_df), None)
    
    # 写入 JSON
    print(f"   正在写入 JSON: {JSON_FILE} ...")
    final_df.to_json(JSON_FILE, orient='records', force_ascii=False)
    print(f"🎉 JSON 数据已生成！大小: {os.path.getsize(JSON_FILE)/1024/1024:.2f} MB")

def main():
    print("="*60)
    print("      📈 A股全市场 Web 版数据生成器")
    print("="*60)
    try:
        market_df = fetch_market_snapshot()
        if not market_df.empty:
            market_df = augment_with_monthly_prices(market_df)
            run_financial_crawler(market_df['代码'].tolist())
            merge_and_export(market_df)
        else:
            print("❌ 行情获取失败。")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()