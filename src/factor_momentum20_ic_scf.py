# coding=utf8
"""
作者: 梁嘉文
项目: G1VENQUANT
功能: 在小市值股票池中构建因子并计算IC值
依赖: 已通过前序脚本下载 ./data/tushare_small_cap_stocks/*.csv
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = "./data/tushare_small_cap_stocks"

def load_all_stock_data():
    """加载所有已下载的小市值股票数据"""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f != 'all_stocks_market_cap.csv']
    stock_data = {}
    for file in tqdm(files, desc="加载股票数据"):
        symbol = file.replace('.csv', '')
        df = pd.read_csv(os.path.join(DATA_DIR, file), parse_dates=['datetime'])
        df = df.sort_values('datetime').reset_index(drop=True)
        stock_data[symbol] = df
    return stock_data

def compute_factors(df):
    """为单只股票计算多个因子"""
    df = df.copy()
    # 收益率
    df['ret_1'] = df['close'].pct_change()  # 当日收益（用于后续计算未来收益）
    
    # ===== 因子1: 20日动量（过去20日累计收益）=====
    df['mom_20'] = df['close'].pct_change(periods=20)
    
    # ===== 因子2: 5日反转（过去5日累计收益，负向预期）=====
    df['reverse_5'] = df['close'].pct_change(periods=5)
    
    # ===== 因子3: 20日平均换手率（用 volume / 流通股本，但无流通股本 → 用成交额替代）=====
    # 若无 amount 字段，可用 volume 近似（注意单位）
    if 'amount' in df.columns and df['amount'].notna().any():
        df['turnover_20'] = df['amount'].rolling(20).mean()
    else:
        df['turnover_20'] = df['volume'].rolling(20).mean()  # 粗略替代
    
    # ===== 因子4: 20日波动率 =====
    df['volatility_20'] = df['ret_1'].rolling(20).std()
    
    # 移除前20行（因子计算需要窗口）
    df = df.iloc[20:].copy()
    return df

def prepare_cross_sectional_data(stock_data):
    """将所有股票数据合并为截面数据（日期 x 股票）"""
    all_dfs = []
    for symbol, df in tqdm(stock_data.items(), desc="计算因子"):
        df_factor = compute_factors(df)
        df_factor['symbol'] = symbol
        all_dfs.append(df_factor[['datetime', 'symbol', 'mom_20', 'reverse_5', 'turnover_20', 'volatility_20', 'ret_1']])
    
    panel = pd.concat(all_dfs, ignore_index=True)
    # 计算下一期收益（用于IC：因子 vs 下期收益）
    panel['future_ret'] = panel.groupby('symbol')['ret_1'].shift(-1)
    return panel

def calculate_ic(panel):
    """计算每个因子的IC序列"""
    factors = ['mom_20', 'reverse_5', 'turnover_20', 'volatility_20']
    ic_results = {}
    
    for factor in factors:
        ic_series = panel.groupby('datetime').apply(
            lambda x: x[factor].corr(x['future_ret'], method='spearman')  # 使用Spearman更稳健
        )
        ic_results[factor] = ic_series.dropna()
    
    return ic_results

def main():
    print("正在加载所有小市值股票数据...")
    stock_data = load_all_stock_data()
    
    print("正在计算因子和未来收益...")
    panel = prepare_cross_sectional_data(stock_data)
    print(f"截面数据规模: {panel.shape}")
    
    print("正在计算IC...")
    ic_results = calculate_ic(panel)
    
    # 输出IC统计
    print("\n===== 因子IC分析结果 =====")
    for factor, ic in ic_results.items():
        ic_mean = ic.mean()
        ic_std = ic.std()
        ir = ic_mean / ic_std if ic_std != 0 else np.nan
        print(f"{factor:15s} | IC均值: {ic_mean:6.4f} | IR: {ir:6.2f} | 有效天数: {len(ic)}")
    
    # 可选：保存IC序列
    ic_df = pd.DataFrame(ic_results)
    ic_df.to_csv(os.path.join(DATA_DIR, "factor_ic_series.csv"))
    print(f"\nIC序列已保存至: {os.path.join(DATA_DIR, 'factor_ic_series.csv')}")

if __name__ == "__main__":
    main()