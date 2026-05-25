import tushare as ts
import pandas as pd

TS_TOKEN = "4858c835fe26ebcb62cf4ac60cb7ddd1f4bc554e9be1096d8d0707ca"
pro = ts.pro_api(TS_TOKEN)

print("="*60)
print("🚀 开始穿透测试：以【贵州茅台 600519.SH】为例进行数据穿透")
print("="*60)

# 1. 测试每日基础行情与估值
try:
    df_val = pro.daily_basic(ts_code='600519.SH', start_date='20260101', end_date='20260523', fields='ts_code,trade_date,pe_ttm,pb,dv_ttm')
    if df_val is not None and not df_val.empty:
        print("✅ 1. daily_basic 接口连接成功！")
        print(f"   最新估值数据片段:\n{df_val.head(1).to_string(index=False)}\n")
    else:
        print("❌ 1. daily_basic 接口返回了空数据，请检查积分是否足够！\n")
except Exception as e:
    print(f"❌ 1. daily_basic 接口报错: {e}\n")

# 2. 测试财务指标综合表
try:
    df_fin = pro.fina_indicator_vip(ts_code='600519.SH', start_date='20240101', end_date='20260523', fields='ts_code,end_date,roe,roic,gpm,npm')
    if df_fin is not None and not df_fin.empty:
        print("✅ 2. fina_indicator_vip 接口连接成功！")
        print(f"   最新财务数据片段:\n{df_fin.head(1).to_string(index=False)}\n")
    else:
        print("❌ 2. fina_indicator_vip 接口返回空数据！可能需要特定 VIP 积分权限，我们将尝试切换到普通版接口...\n")
        
        # 备用：尝试普通版财务接口
        df_fin_normal = pro.fina_indicator(ts_code='600519.SH', start_date='20240101', end_date='20260523', fields='ts_code,end_date,roe,roic,gpm,npm')
        if df_fin_normal is not None and not df_fin_normal.empty:
            print("💡 备用方案成功：普通版 fina_indicator 接口有数据！")
            print(f"   最新财务数据片段:\n{df_fin_normal.head(1).to_string(index=False)}\n")
except Exception as e:
    print(f"❌ 2. 财务接口报错: {e}\n")

print("="*60)
