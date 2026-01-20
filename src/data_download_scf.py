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
TOKEN = "12e92cd92a1346d0cb14cff8cd574f709d642b52d15c6f5ebb2255ce"
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

def get_last_trade_date():
    """获取最近一个交易日（格式 YYYYMMDD）"""
    df = pro.trade_cal(exchange='SSE', start_date='20260101', end_date='20261231', is_open=1)
    last_date = df['cal_date'].iloc[-1]
    print(f"最近一个交易日: {last_date}")
    return last_date

def download_market_cap_data():
    """获取前一个交易日全市场股票流通市值数据"""
    df = pro.daily_basic(ts_code='', trade_date=get_last_trade_date(), fields='ts_code,trade_date,total_mv,circ_mv')
    print(f"获取到 {len(df)} 只股票的市值数据（日期: {get_last_trade_date()}）")
    print(df.head()) # 预览数据
    # 保存csv文件
    csv_path = os.path.join(DATA_DIR, "all_stocks_market_cap.csv")
    df.to_csv(csv_path, index=False)
    return df

def get_st_stock_set():
    """获取指定交易日的 ST/*ST 股票代码集合"""
    try:
        st_df = pro.stock_st(trade_date=get_last_trade_date())
        return set(st_df['ts_code'].tolist())
        print(f"获取到 {len(st_df)} 只 ST 股票（日期: {get_last_trade_date()}）")       
    except Exception as e:
        print(f"⚠️ 获取 ST 股票失败（{get_last_trade_date()}）: {e}，将跳过 ST 排除")
        return set()
    

def filter_small_cap_stocks(df):
    """筛选小市值股票"""
    df = df[df['total_mv'] < MARKET_CAP_THRESHOLD_BILLION * 10000] # 转换为万元单位
    print(f"筛选出小于 {MARKET_CAP_THRESHOLD_BILLION} 亿元市值的股票数量: {len(df)}")
    # 排除ST股票
    if EXCLUDE_ST:
        st_stocks = get_st_stock_set()
        df = df[~df['ts_code'].isin(st_stocks)]
    print(f"共筛选出符合条件的小市值股票数量: {len(df)}")
    # 只保留股票代码
    df = df[['ts_code']]
    print(df.head()) # 预览筛选结果
    return df

def download_stock_data(ts_code, start, end):
    """下载单只股票的前复权日线数据(使用Tushare pro接口)"""
    try:
        symbol = ts_code.split('.')[0]  # 提取纯数字代码，如 '000001'
        df = ts.pro_bar(
            ts_code=ts_code,
            adj='qfq', # 前复权
            start_date=start,
            end_date=end,
            freq='D' # 日线
        )
        # 检查数据是否为空
        if df is None or df.empty:
            return False

        # 整理字段
        df = df.rename(columns={
            'trade_date': 'datetime',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume',
            'amount': 'amount'
        })
        # 转换日期格式并排序
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d')
        df = df.sort_values('datetime').reset_index(drop=True)

        # 保存为 CSV
        output_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        return True
    # 出错处理
    except Exception as e:
        print(f"\n⚠️ 下载 {ts_code} 出错: {e}")
        return False

def main():
    print("正在获取前一个交易日全市场股票流通市值数据...")
    df = download_market_cap_data()
    df = filter_small_cap_stocks(df)
    selected_stocks = df['ts_code'].tolist()

    print("正在批量下载小市值股票日线数据...")
    for stock in tqdm(selected_stocks):
        download_stock_data(stock, START_DATE, END_DATE)

if __name__ == "__main__":
    main()


        