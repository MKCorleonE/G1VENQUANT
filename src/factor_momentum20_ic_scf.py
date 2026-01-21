# coding=utf8
"""
作者: 梁嘉文
项目: G1VENQUANT
功能: 在小市值股票池的前15支股票中构建因子并计算IC值（快速验证）
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
import seaborn as sns

# ====== 关键参数：设定分析的时间窗口 ======
START_ANALYSIS_DATE = "2021-01-01"   # 可改为 "2023-01-01" 用最近3年
END_ANALYSIS_DATE = "2025-12-31"     # 与你下载数据的截止日一致

DATA_DIR = "./data/tushare_small_cap_stocks"  # 请根据实际情况修改数据目录
FIGURE_DIR = "./figures" # 图表保存目录

def load_all_stock_data(n_stocks=15):
    """加载前 n_stocks 支小市值股票数据（按文件名排序）"""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f != 'all_stocks_market_cap.csv']
    if not files:
        raise FileNotFoundError("未找到股票数据文件，请确认已运行数据下载脚本")
    files = sorted(files)[:n_stocks]
    print(f"将分析以下 {len(files)} 支股票: {', '.join([f.replace('.csv','') for f in files])}")
    
    stock_data = {}
    for file in tqdm(files, desc=f"加载前 {n_stocks} 支股票"):
        symbol = file.replace('.csv', '')
        df = pd.read_csv(os.path.join(DATA_DIR, file), parse_dates=['datetime'])
        df = df.sort_values('datetime').reset_index(drop=True)
        stock_data[symbol] = df
    return stock_data

def compute_factors(df):
    """为单只股票计算多个因子"""
    df = df.copy() # 避免修改原始数据
    df['ret_1'] = df['close'].pct_change()
    
    # 因子1: 20日动量
    df['mom_20'] = df['close'].pct_change(periods=20)
    # 因子2: 5日反转
    df['reverse_5'] = df['close'].pct_change(periods=5)
    # 因子3: 20日平均成交额（替代换手率）
    df['turnover_20'] = df['amount'].rolling(20).mean() if 'amount' in df.columns else df['volume'].rolling(20).mean()
    # 因子4: 20日波动率
    df['volatility_20'] = df['ret_1'].rolling(20).std()
    
    return df.iloc[20:].copy()  # 去掉前20天（窗口不足）

def prepare_cross_sectional_data(stock_data):
    all_dfs = []
    for symbol, df in stock_data.items():
        df_factor = compute_factors(df)
        df_factor['symbol'] = symbol
        all_dfs.append(df_factor[['datetime', 'symbol', 'mom_20', 'reverse_5', 'turnover_20', 'volatility_20', 'ret_1']])
    
    panel = pd.concat(all_dfs, ignore_index=True)
    panel['future_ret'] = panel.groupby('symbol')['ret_1'].shift(-1)
    # 仅保留分析期内的数据
    panel = panel[
        (panel['datetime'] >= START_ANALYSIS_DATE) & 
        (panel['datetime'] <= END_ANALYSIS_DATE)
    ].copy()
    return panel.dropna(subset=['future_ret'])  # 删除最后一天（无未来收益）

def calculate_ic(panel):
    factors = ['mom_20', 'reverse_5', 'turnover_20', 'volatility_20']
    ic_results = {}
    for factor in factors:
        ic_series = panel.groupby('datetime').apply(
            lambda x: x[factor].corr(x['future_ret'], method='spearman')
        )
        ic_results[factor] = ic_series.dropna()
    return ic_results

def plot_factors_trend(panel):
    factors = ['mom_20', 'reverse_5', 'turnover_20', 'volatility_20']
    
    for factor in factors:
        plt.figure(figsize=(14, 7))
        # 设置风格
        sns.set_theme(style="whitegrid")
        
        # 对于每个因子，计算每个月的平均值以观察长期趋势
        monthly_avg = panel.groupby([pd.Grouper(key='datetime', freq='M')])[factor].mean()
        
        # 绘制时间序列图
        sns.lineplot(data=monthly_avg)
        plt.title(f'{factor} 的周期性趋势')
        plt.xlabel('日期')
        plt.ylabel(factor)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        plt.savefig(os.path.join(FIGURE_DIR, f"{factor}_trend.png"), dpi=150)

def main():
    print("🚀 开始小样本因子IC分析（前15支股票）...")
    stock_data = load_all_stock_data(n_stocks=15)
    
    print("正在计算因子...")
    panel = prepare_cross_sectional_data(stock_data)
    print(f"✅ 截面数据构建完成: {panel['datetime'].nunique()} 个交易日, {panel.shape[0]} 条记录")
    
    print("正在计算IC...")
    ic_results = calculate_ic(panel)
    
    print("\n" + "="*60)
    print("📊 因子IC分析结果（小样本）")
    print("="*60)
    for factor, ic in ic_results.items():
        if len(ic) == 0:
            print(f"{factor:15s} | 无有效IC数据")
            continue
        ic_mean = ic.mean()
        ic_std = ic.std()
        ir = ic_mean / ic_std if ic_std > 1e-6 else np.nan
        t_stat = ic_mean / (ic_std / np.sqrt(len(ic))) if ic_std > 1e-6 else np.nan
        print(f"{factor:15s} | IC均值: {ic_mean:7.4f} | IR: {ir:6.2f} | t-stat: {t_stat:6.2f} | 天数: {len(ic)}")
    
    if 'panel' in locals():
        plot_factors_trend(panel)

if __name__ == "__main__":
    main()