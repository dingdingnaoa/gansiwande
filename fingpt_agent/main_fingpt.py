import os
import sys
import json
import time
import pandas as pd
import tushare as ts

print("🚀 [FinGPT Independent Agent] 赛博量化舆情情感分析智能体正在启动...")

TOKEN = os.getenv('TUSHARE_TOKEN', '4858c835fe26ebcb62cf4ac60cb7ddd1f4bc554e9be1096d8d0707ca').strip()
ts.set_token(TOKEN)
pro = ts.pro_api()

target_stocks = {
    "600519.SH": "贵州茅台",
    "002594.SZ": "比亚迪",
    "300750.SZ": "宁德时代"
}

def fetch_stock_news(ts_code, stock_name):
    print(f"📥 正在为 [{stock_name}] 动态拦截全网最新财经舆情流...")
    try:
        df = pro.news(src='sina', start_date=(pd.Timestamp.now() - pd.Timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S'),
                      end_date=pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'), fields='title,content')
        if df is not None and not df.empty:
            df_filtered = df[df['title'].str.contains(stock_name) | df['content'].str.contains(stock_name)]
            return df_filtered.head(3)['title'].tolist()
    except Exception as e:
        print(f"⚠️ 新闻舆情拦截失败: {e}")
    return ["暂无突发宏观异动新闻。"]

def fingpt_mock_sentiment_llm(news_list):
    score = 0
    positive_words = ['增长', '新高', '破局', '看好', '反弹', '买入', '增持', '舒缓', '通过', '健康']
    negative_words = ['回撤', '暴跌', '预警', '下滑', '亏损', '立案', '未来函数', '熔断', '报错', '违约']
    
    combined_text = "".join(news_list)
    pos_count = sum(1 for w in positive_words if w in combined_text)
    neg_count = sum(1 for w in negative_words if w in combined_text)
    
    if pos_count > neg_count:
        return 1, "FinGPT 判定：基本面信心强劲，情绪偏向乐观（Positive）"
    elif neg_count > pos_count:
        return -1, "FinGPT 判定：技术面遭遇阶段性扰动，情绪偏向谨慎（Negative）"
    else:
        return 0, "FinGPT 判定：筹码分布稳定，舆情多为空白或中性平衡（Neutral）"

def run_independent_pipeline():
    fingpt_results = {}
    for code, name in target_stocks.items():
        news = fetch_stock_news(code, name)
        score, report = fingpt_mock_sentiment_llm(news)
        fingpt_results[code] = {
            "stock_name": name,
            "sentiment_score": score,
            "ai_analysis": report,
            "latest_news_sample": news[0] if news else "无"
        }
        time.sleep(0.2)
        
    output_path = 'fingpt_agent/fingpt_sentiment.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(fingpt_results, f, ensure_ascii=False, indent=4)
    print(f"✅ [SUCCESS] 情感因子已安全落盘至 {output_path}，模块处于随时可被前端调用状态！")

if __name__ == '__main__':
    run_independent_pipeline()
