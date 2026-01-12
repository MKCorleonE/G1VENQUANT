# coding=utf8
"""
G1VENQUANT EDA: Exploratory Data Analysis for selected stocks
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from matplotlib import rcParams

# ======= 配置区 =======
DATA_DIR = "./data/tushare_selected_stocks" # 数据存储目录
OUTPUT_FIG_DIR = "./figures" # 图表存储目录

# 股票代码 → 中文名映射（请根据你的实际股票修改）
STOCK_NAMES = {
    '000001': '平安银行',
    '600519': '贵州茅台',
    '300750': '宁德时代',
    '000858': '五粮液',
    '601318': '中国平安'
}

# 创建输出目录
os.makedirs(OUTPUT_FIG_DIR, exist_ok=True)

# ======================
# 数据加载
# ======================

def load_all_stocks():
    """从 CSV 加载所有股票数据"""
    # 验证数据目录是否存在
    if not os.path.exists(DATA_DIR):
        raise FileNotFoundError(f"数据目录不存在: {os.path.abspath(DATA_DIR)}")

    # 新建字典存储股票数据
    stocks = {}

    files = []
    for f in os.listdir(DATA_DIR):  # 遍历目录下所有文件和文件夹
        if f.endswith('.csv'):       # 检查是否以 .csv 结尾
            files.append(f)          # 符合条件的添加到列表
    # 验证是否找到任何 CSV 文件
    if not files:
        raise ValueError(f"目录 {DATA_DIR} 中没有 CSV 文件！")

    for file in files:
        symbol = file.replace('.csv', '')
        df = pd.read_csv(os.path.join(DATA_DIR, file))
        df['datetime'] = pd.to_datetime(df['datetime']) # 转换为日期时间格式
        df.set_index('datetime', inplace=True) # 设置日期为索引
        df.sort_index(inplace=True) # 按日期排序
        stocks[symbol] = df # 存入字典
        print(f"✓ 加载 {symbol} ({STOCK_NAMES.get(symbol, symbol)}) - {len(df)} 条记录")

    return stocks

# ======================
# 可视化函数
# ======================

def plot_price_and_volume(stocks):
    """单只股票：价格 + 成交量"""
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

        # 收盘价
        axes[0].plot(df.index, df['close'], color='tab:blue', linewidth=1)
        axes[0].set_title(f'{name} ({symbol}) — 复权收盘价', fontsize=14)
        axes[0].set_ylabel('价格 (元)')

        # 成交量（单位：手）
        axes[1].bar(df.index, df['volume'], width=1, color='tab:orange', alpha=0.7)
        axes[1].set_ylabel('成交量 (手)')
        axes[1].set_xlabel('日期')

        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_FIG_DIR, f"{symbol}_price_volume.png"), dpi=150, bbox_inches='tight')
        plt.close()

def plot_normalized_prices(stocks):
    """多股归一化对比"""
    plt.figure(figsize=(14, 8))
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        norm_price = df['close'] / df['close'].iloc[0]
        plt.plot(df.index, norm_price, label=name, linewidth=2)

    plt.title('股票价格走势对比（归一化，起始值=1）', fontsize=16)
    plt.ylabel('归一化价格')
    plt.xlabel('日期')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "normalized_prices.png"), dpi=150, bbox_inches='tight')
    plt.close()

def plot_return_distribution(stocks):
    """收益率分布（直方图 + 正态拟合）"""
    n = len(stocks)
    cols = 2
    rows = (n + 1) // 2
    fig, axes = plt.subplots(rows, cols, figsize=(14, 5 * rows))
    if n == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    for idx, (symbol, df) in enumerate(stocks.items()):
        name = STOCK_NAMES.get(symbol, symbol)
        returns = df['close'].pct_change().dropna()
        mu, sigma = returns.mean(), returns.std()

        # 直方图
        axes[idx].hist(returns, bins=50, density=True, alpha=0.7, color='skyblue', edgecolor='k')

        # 正态拟合
        x = np.linspace(returns.min(), returns.max(), 100)
        axes[idx].plot(x, stats.norm.pdf(x, mu, sigma), 'r--', linewidth=2, label='正态拟合')

        axes[idx].set_title(f'{name} 日收益率分布\n均值={mu:.4f}, 标准差={sigma:.4f}')
        axes[idx].legend()

    # 隐藏多余子图
    for j in range(n, len(axes)):
        axes[j].axis('off')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "return_distributions.png"), dpi=150, bbox_inches='tight')
    plt.close()

def plot_rolling_volatility(stocks, window=20):
    """滚动波动率（年化）"""
    plt.figure(figsize=(14, 8))
    for symbol, df in stocks.items():
        name = STOCK_NAMES.get(symbol, symbol)
        returns = df['close'].pct_change()
        rolling_std = returns.rolling(window=window).std()
        annualized_vol = rolling_std * np.sqrt(252)
        plt.plot(annualized_vol.index, annualized_vol, label=name, linewidth=1.5)

    plt.title(f'{window}日滚动年化波动率', fontsize=16)
    plt.ylabel('年化波动率')
    plt.xlabel('日期')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_FIG_DIR, "rolling_volatility.png"), dpi=150, bbox_inches='tight')
    plt.close()

# ======================
# 主程序
# ======================

def main():
    print("📊 开始 A 股数据探索性分析 (EDA)...")
    try:
        stocks = load_all_stocks()
        print(f"\n📈 共加载 {len(stocks)} 只股票，开始生成图表...\n")

        plot_price_and_volume(stocks)
        plot_normalized_prices(stocks)
        plot_return_distribution(stocks)
        plot_rolling_volatility(stocks)

        print(f"\n✅ 所有图表已成功保存至：{os.path.abspath(OUTPUT_FIG_DIR)}")
        print("📁 包含：")
        print("   • 单股价格+成交量图")
        print("   • 多股归一化走势对比")
        print("   • 收益率分布直方图")
        print("   • 滚动波动率时序图")

    except Exception as e:
        print(f"❌ 发生错误: {e}")
        raise

if __name__ == "__main__":
    main()