# coding=utf8
"""
Created on Thu Sep 5 11:02:00 2025
作者: 梁嘉文
项目: G1VENQUANT
功能: 自动筛选小市值股票并批量下载日线数据
"""

import os
import time
import tushare as ts
import pandas as pd
from tqdm import tqdm

# ======= 配置区 =======
TOKEN = "9bb81649792cc92d8e0ed2a5789d47b4bcd74a53b224ce44f3a4e0e6"
DATA_DIR = "./data/tushare_small_cap_stocks"
START_DATE = "20150101"
END_DATE = "20251231"

# 小市值策略参数
MARKET_CAP_THRESHOLD_BILLION = 50  # 单位：亿元（总市值 < X 亿）
EXCLUDE_ST = True                   # 是否排除 ST/*ST 股票
MIN_LISTING_DAYS = 365             # 上市至少多少天（避免新股）

# 创建数据目录
os.makedirs(DATA_DIR, exist_ok=True)

# 初始化 Tushare
ts.set_token(TOKEN)
pro = ts.pro_api()

def get_recent_trade_dates(n=5):
    """获取最近 n 个交易日（用于稳健筛选）"""
    df = pro.trade_cal(exchange='SSE', start_date='20250101', end_date='20251231')
    df = df[df['is_open'] == 1].sort_values('cal_date', ascending=False)
    return df['cal_date'].head(n).tolist()

def get_small_cap_stock_pool(trade_dates, market_cap_billion, exclude_st=True, min_listing_days=365):
    """
    获取稳定的小市值股票池（在多个交易日均满足条件）
    """
    print("🔍 正在筛选小市值股票池...")
    
    # 获取股票基本信息（用于排除ST、新股）
    stock_info = pro.stock_basic(fields='ts_code, name, list_date')
    stock_info['list_date'] = pd.to_datetime(stock_info['list_date'], format='%Y%m%d')
    cutoff_date = pd.Timestamp('20251231') - pd.Timedelta(days=min_listing_days)
    stock_info['is_new'] = stock_info['list_date'] > cutoff_date

    all_sets = []
    for date in trade_dates:
        try:
            # 获取当日基本面数据（含市值）
            df_basic = pro.daily_basic(
                trade_date=date,
                fields='ts_code, total_mv'
            )
            # 市值单位：万元 → 转为亿元比较
            cap_threshold_wan = market_cap_billion * 10000
            small_today = set(df_basic[df_basic['total_mv'] < cap_threshold_wan]['ts_code'])

            # 合并基本信息做过滤
            merged = pd.DataFrame({'ts_code': list(small_today)}).merge(stock_info, on='ts_code', how='left')
            
            if exclude_st:
                merged = merged[~merged['name'].str.contains(r'ST|退', na=False)]
            if min_listing_days > 0:
                merged = merged[~merged['is_new']]

            valid_codes = set(merged['ts_code'])
            all_sets.append(valid_codes)
            time.sleep(0.2)  # 防止调用过快
        except Exception as e:
            print(f"⚠️ 获取 {date} 的数据失败: {e}")
            continue

    if not all_sets:
        raise ValueError("未能获取任何有效交易日的小市值股票")

    # 取交集：在所有日期都满足小市值条件的股票（更稳健）
    final_set = all_sets[0]
    for s in all_sets[1:]:
        final_set &= s

    print(f"✅ 筛选出 {len(final_set)} 只稳定小市值股票（总市值 < {market_cap_billion} 亿元）")
    return sorted(list(final_set))

def download_stock_data(ts_code, start, end):
    """下载单只股票的前复权日线数据"""
    try:
        symbol = ts_code.split('.')[0]
        df = ts.pro_bar(
            ts_code=ts_code,
            adj='qfq',
            start_date=start,
            end_date=end,
            freq='D'
        )
        if df is None or df.empty:
            return False

        df = df.rename(columns={
            'trade_date': 'datetime',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume',
            'amount': 'amount'
        })
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d')
        df = df.sort_values('datetime').reset_index(drop=True)

        output_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        return True

    except Exception as e:
        print(f"\n⚠️ 下载 {ts_code} 出错: {e}")
        return False

# ======= 主程序 ========
def main():
    # Step 1: 获取最近几个交易日
    recent_dates = get_recent_trade_dates(n=5)  # 可调整为3或5
    print(f"使用最近交易日进行筛选: {recent_dates}")

    # Step 2: 自动筛选小市值股票池
    SELECTED_STOCKS = get_small_cap_stock_pool(
        trade_dates=recent_dates,
        market_cap_billion=MARKET_CAP_THRESHOLD_BILLION,
        exclude_st=EXCLUDE_ST,
        min_listing_days=MIN_LISTING_DAYS
    )

    total = len(SELECTED_STOCKS)
    print(f"准备下载 {total} 只小市值股票的数据...")
    
    success_count = 0
    for ts_code in tqdm(SELECTED_STOCKS, desc="Downloading"):
        symbol = ts_code.split('.')[0]
        file_path = os.path.join(DATA_DIR, f"{symbol}.csv")

        if os.path.exists(file_path):
            continue  # 断点续传

        if download_stock_data(ts_code, START_DATE, END_DATE):
            success_count += 1
        time.sleep(0.2)  # 防止 Tushare 接口限频

    print(f"\n✅ 下载完成！成功: {success_count}/{total} 只股票")
    print(f"数据保存在: {os.path.abspath(DATA_DIR)}")

if __name__ == "__main__":
    main()