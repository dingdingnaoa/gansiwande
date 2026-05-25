import json
import os
import pandas as pd

def advanced_quality_scan():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, 'data.json')
    
    if not os.path.exists(json_path):
        print(f"❌ 找不到文件，请确认 data.json 是否在当前目录下: {json_path}")
        return

    print("="*80)
    print("        📊 A股全市场股票数据【完整性与断档深度扫描】")
    print("="*80)

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    total_stocks = len(df)
    
    base_fields = ['代码', 'symbol', '名称', 'area', 'industry', 'list_date']
    print(f"\n📋 [1] 基础字段完整度 (目标: {total_stocks} 只股票):")
    for field in base_fields:
        if field in df.columns:
            missing = df[field].isna().sum()
            status = "✅ 完美" if missing == 0 else f"❌ 缺失 {missing} 条"
            print(f"   - {field:<10}: {status}")
        else:
            print(f"   - ❌ 严重错误: 核心字段 【{field}】 在 JSON 中完全不存在！")

    price_cols = sorted([col for col in df.columns if '_月初价' in col], reverse=True)
    total_months = len(price_cols)
    print(f"\n📊 [2] 历史价格字段结构 (扫描到 {total_months} 个月份):")
    if total_months > 0:
        print(f"   - 时间跨度: 从 {price_cols[-1].replace('_月初价','')} 到 {price_cols[0].replace('_月初价','')}")
    else:
        print("   - ❌ 错误: 未找到任何价格历史字段！")
        return

    print(f"\n🚨 [3] 核心异常排查列表:")
    df['missing_count'] = df[price_cols].isna().sum(axis=1)
    
    all_missing = df[df['missing_count'] == total_months]
    if not all_missing.empty:
        print(f"   💥 严重异常：有 {len(all_missing)} 只股票完全没有历史价格数据！")
        print(f"      示例代码: {all_missing['代码'].head(10).tolist()}")
    else:
        print("   ✅ 优秀：不存在“完全没有价格”的股票。")

    ordered_price_cols = sorted(price_cols)
    gap_stocks = []
    
    for idx, row in df.iterrows():
        prices = row[ordered_price_cols].values
        valid_indices = [i for i, v in enumerate(prices) if pd.notna(v)]
        if len(valid_indices) > 1:
            first_valid = valid_indices[0]
            last_valid = valid_indices[-1]
            if pd.isna(prices[first_valid:last_valid+1]).any():
                gaps = [ordered_price_cols[first_valid + i].replace('_月初价','') 
                        for i, v in enumerate(prices[first_valid:last_valid+1]) if pd.isna(v)]
                gap_stocks.append({
                    "代码": row['代码'], 
                    "名称": row['名称'], 
                    "上市日期": row['list_date'],
                    "断档月份": gaps[:3]
                })

    if gap_stocks:
        print(f"   ⚠️ 异常断档：有 {len(gap_stocks)} 只老股票在上市后，历史价格中途出现“断层/丢月”！")
        print("      典型断档股票示例 (前5只):")
        for s in gap_stocks[:5]:
            print(f"      - {s['代码']} ({s['名称']}) | 上市日期: {s['上市日期']} | 断档月份如: {s['断档月份']}")
    else:
        print("   ✅ 完美：所有股票的历史时序完全连续，无中途断档（未上市前的自然空值除外）。")

    recent_stocks = df[(df['missing_count'] > 0) & (df['missing_count'] < total_months)]
    gap_codes = [s['代码'] for s in gap_stocks]
    pure_new_stocks = recent_stocks[~recent_stocks['代码'].isin(gap_codes)]
    
    print(f"\n📉 [4] 正常数据稀疏度:")
    print(f"   - 历史数据 100% 全满的股票: {total_stocks - len(recent_stocks)} 只")
    print(f"   - 因近年新上市导致前面月份为 null 的股票: {len(pure_new_stocks)} 只")
    print("="*80)

if __name__ == "__main__":
    advanced_quality_scan()
