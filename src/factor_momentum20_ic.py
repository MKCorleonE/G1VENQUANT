# coding=utf8
"""
第一步因子研究：20日动量因子 + IC 分析
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import spearmanr

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# ======= 配置区 =======
DATA_DIR = "./data/tushare_selected_stocks"
OUTPUT_DIR = "./factor_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

STOCK_NAMES = {
    '000001': '平安银行',
    '600519': '贵州茅台',
    '300750': '宁德时代',
    '000858': '五粮液',
    '601318': '中国平安'
}

def load_all_data():
    """加载所有股票数据，并合并成面板数据 (date x symbol)"""
    all_dfs = []
    # 遍历数据目录下所有CSV文件
    for file in os.listdir(DATA_DIR):
        if not file.endswith('.csv'):
            continue
        symbol = file.replace('.csv', '')
        df = pd.read_csv(os.path.join(DATA_DIR, file))
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['symbol'] = symbol
        all_dfs.append(df[['datetime', 'symbol', 'close']]) # 只保留需要的列
    
    # 合并所有数据为一个面板数据
    panel = pd.concat(all_dfs, ignore_index=True)
    panel = panel.sort_values(['symbol', 'datetime']).reset_index(drop=True)
    print(f"面板数据结构预览:\n{panel.head()}")
    return panel

def calculate_momentum_factor(panel, lookback=20):
    """计算动量因子：过去 lookback 日的收益率（使用 transform 保证索引对齐）"""
    panel = panel.copy()
    panel = panel.sort_values(['symbol', 'datetime']).reset_index(drop=True)
    
    # 计算每日简单收益率（用于 future_return）
    panel['return'] = panel.groupby('symbol')['close'].pct_change()
    
    # ✅ 关键修复：使用 transform 计算动量因子
    panel['mom_factor'] = panel.groupby('symbol')['close'].transform(
        lambda x: x.pct_change(periods=lookback)
    )
    
    # 未来一期收益（避免前视偏差）
    panel['future_return'] = panel.groupby('symbol')['return'].shift(-1)
    
    return panel

def calculate_ic(panel):
    """计算每日 IC（Spearman 秩相关系数）"""
    ic_list = []
    dates = sorted(panel['datetime'].dropna().unique())
    
    for date in dates:
        df_date = panel[panel['datetime'] == date].copy()
        # 去除缺失值
        df_date = df_date.dropna(subset=['mom_factor', 'future_return'])
        
        if len(df_date) < 2:
            continue
            
        try:
            ic, _ = spearmanr(df_date['mom_factor'], df_date['future_return'])
            ic_list.append({'date': date, 'ic': ic})
        except Exception as e:
            print(f"⚠️ 计算 {date} 的 IC 时出错: {e}")
            continue
    
    if not ic_list:
        raise ValueError("未能计算任何有效 IC 值，请检查数据")
        
    ic_df = pd.DataFrame(ic_list)
    ic_df['date'] = pd.to_datetime(ic_df['date'])
    ic_df.set_index('date', inplace=True)
    return ic_df

def plot_ic_analysis(ic_df):
    """绘制 IC 分析图"""
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    
    # IC 时间序列
    axes[0].plot(ic_df.index, ic_df['ic'], color='tab:blue', alpha=0.7, linewidth=1)
    axes[0].axhline(y=0, color='k', linestyle='--', linewidth=0.8)
    axes[0].set_title('动量因子每日 IC（Spearman 秩相关）', fontsize=14)
    axes[0].set_ylabel('IC')
    
    # IC 分布
    axes[1].hist(ic_df['ic'], bins=30, color='skyblue', edgecolor='k', alpha=0.8)
    mean_ic = ic_df['ic'].mean()
    axes[1].axvline(x=mean_ic, color='r', linestyle='--', label=f'均值 = {mean_ic:.4f}')
    axes[1].set_title('IC 分布', fontsize=14)
    axes[1].set_xlabel('IC')
    axes[1].legend()
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "momentum_ic_analysis.png"), dpi=150, bbox_inches='tight')
    plt.close()

def main():
    print("📊 开始动量因子构建与 IC 分析...")
    
    # 1. 加载数据
    panel = load_all_data()
    print(f"共加载 {panel['symbol'].nunique()} 只股票，{len(panel)} 条记录")
    
    # 2. 计算因子
    panel = calculate_momentum_factor(panel, lookback=20)
    print("✅ 动量因子计算完成")
    
    # 3. 计算 IC
    ic_df = calculate_ic(panel)
    mean_ic = ic_df['ic'].mean()
    ir = mean_ic / ic_df['ic'].std() if ic_df['ic'].std() != 0 else np.nan
    
    print(f"\n📈 IC 分析结果:")
    print(f"   平均 IC: {mean_ic:.4f}")
    print(f"   ICIR (信息比率): {ir:.4f}")
    print(f"   有效天数: {len(ic_df)}")
    
    # 4. 可视化
    plot_ic_analysis(ic_df)
    print(f"\n✅ 结果已保存至: {os.path.abspath(OUTPUT_DIR)}")
    
    # 5. 保存中间数据
    panel.to_csv(os.path.join(OUTPUT_DIR, "factor_panel.csv"), index=False)
    ic_df.to_csv(os.path.join(OUTPUT_DIR, "ic_series.csv"))
    print("📁 已保存因子面板和 IC 序列")

if __name__ == "__main__":
    main()