# coding=utf8
"""
G1VENQUANT EDA: Exploratory Data Analysis for selected stocks
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# 设置中文字体（避免中文乱码）
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style("whitegrid")

# ===== 配置 =====
DATA_DIR = "./data/tushare_selected_stocks"
OUTPUT_FIG_DIR = "./figures"
os.makedirs(OUTPUT_FIG_DIR, exist_ok=True)

# 股票代码映射（用于显示中文名）
STOCK_NAMES = {
    '000001': '平安银行',
    '600519': '贵州茅台',
    '300750': '宁德时代',
    '000858': '五粮液',
    '601318': '中国平安'
}

def load_all_stocks():
    """加载所有股票数据"""
    stocks = {}
    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            symbol = file.replace('.csv', '')
            df = pd.read_csv(os.path.join(DATA_DIR, file))
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)
            stocks[symbol] = df
    return stocks

def plot_price_and_volume(stocks):
    """为每只股票画价格+成交量子图"""
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        # 收盘价
        axes[0].plot(df.index, df['close'], color='tab:blue')
        axes[0].set_title(f'{name} ({symbol}) - 复权收盘价', fontsize=14)
        axes[0].set_ylabel('价格 (元)')
        
        # 成交量（单位：手 → 转为万股便于阅读）
        vol = df['volume'] / 100  # 手 → 百股，但通常直接画原始或缩放
        axes[1].bar(df.index, vol, width=1, color='tab:orange', alpha=0.7)
        axes[1].set_ylabel('成交量 (手)')
        axes[1].set_xlabel('日期')
        
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_FIG_DIR, f"{symbol}_price_volume.png"), dpi=150)
        plt.close()

def plot_normalized_prices(stocks):
    """多股票归一化价格对比（从1开始）"""
    plt.figure(figsize=(14, 8))
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        # 归一化：首日价格为1
        norm_price = df['close'] / df['close'].iloc[0]
        plt.plot(df.index, norm_price, label=name)
    
    plt.title('股票价格走势对比（归一化）', fontsize=16)
    plt.ylabel('归一化价格（起始=1）')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "normalized_prices.png"), dpi=150)
    plt.close()

def plot_return_distribution(stocks):
    """画收益率分布（直方图 + 正态拟合）"""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    
    for idx, (symbol, df) in enumerate(stocks.items()):
        if idx >= 6:
            break
        name = STOCK_NAMES.get(symbol, symbol)
        returns = df['close'].pct_change().dropna()
        
        ax = axes[idx]
        # 直方图
        ax.hist(returns, bins=50, density=True, alpha=0.7, color='skyblue', edgecolor='k')
        
        # 正态分布拟合
        mu, sigma = returns.mean(), returns.std()
        x = np.linspace(returns.min(), returns.max(), 100)
        ax.plot(x, stats.norm.pdf(x, mu, sigma), 'r--', linewidth=2, label='正态拟合')
        
        ax.set_title(f'{name} 日收益率分布\nμ={mu:.4f}, σ={sigma:.4f}')
        ax.legend()
    
    # 隐藏多余子图
    for j in range(len(stocks), 6):
        axes[j].axis('off')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "return_distributions.png"), dpi=150)
    plt.close()

def plot_rolling_volatility(stocks, window=20):
    """滚动波动率（20日年化）"""
    plt.figure(figsize=(14, 8))
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        returns = df['close'].pct_change()
        rolling_std = returns.rolling(window=window).std()
        annualized_vol = rolling_std * np.sqrt(252)  # 年化
        plt.plot(annualized_vol.index, annualized_vol, label=name)
    
    plt.title(f'{window}日滚动年化波动率', fontsize=16)
    plt.ylabel('年化波动率')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "rolling_volatility.png"), dpi=150)
    plt.close()

def main():
    print("正在加载股票数据...")
    stocks = load_all_stocks()
    print(f"共加载 {len(stocks)} 只股票")
    
    print("正在生成可视化图表...")
    plot_price_and_volume(stocks)
    plot_normalized_prices(stocks)
    plot_return_distribution(stocks)
    plot_rolling_volatility(stocks)
    
    print(f"✅ 所有图表已保存至: {os.path.abspath(OUTPUT_FIG_DIR)}")

if __name__ == "__main__":
    main()