# coding=utf8
"""
第二步：基于20日动量因子的简单回测
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# ======= 配置区 =======
FACTOR_PANEL_PATH = "./factor_results/factor_panel.csv"
OUTPUT_DIR = "./factor_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 策略参数
TOP_N = 2  # 每期做多动量最高的 N 只股票
REBALANCE_FREQ = 'D'  # 调仓频率：'D'=日频，'W'=周频，'M'=月频（这里用日频）

def load_factor_panel():
    """加载因子面板数据"""
    panel = pd.read_csv(FACTOR_PANEL_PATH)
    panel['datetime'] = pd.to_datetime(panel['datetime'])
    return panel

def generate_signals(panel, top_n=2):
    """生成交易信号：每期选择动量最高的 top_n 只股票"""
    panel = panel.copy()
    
    # 移除缺失因子值
    panel = panel.dropna(subset=['mom_factor', 'future_return'])
    
    # 按日期分组，对每期股票按动量因子降序排序
    def select_top(group):
        group = group.sort_values('mom_factor', ascending=False)
        group['position'] = 0.0

        # 获取 'position' 列的索引位置
        col_index = group.columns.get_loc('position')
        # 选择前 top_n 行的 'position' 列
        selected_cells = group.iloc[:top_n, col_index]
        # 为这些单元格分配等权重
        selected_cells[:] = 1.0 / top_n
        
        return group
    
    panel = panel.groupby('datetime', group_keys=False).apply(select_top)
    return panel

def calculate_portfolio_returns(panel):
    """根据持仓计算组合每日收益"""
    # future_return 已经是下一期的实际收益率（无前视偏差）
    panel['strategy_return'] = panel['position'] * panel['future_return']
    
    # 按日期聚合组合收益
    daily_pnl = panel.groupby('datetime')['strategy_return'].sum().to_frame()
    daily_pnl = daily_pnl.dropna()
    daily_pnl.index.name = 'date'
    
    # 计算累计收益
    daily_pnl['cum_return'] = (1 + daily_pnl['strategy_return']).cumprod()
    
    # 基准：等权持有所有股票（作为简单对比）
    benchmark = panel.groupby('datetime')['future_return'].mean().to_frame(name='benchmark_return')
    benchmark['cum_benchmark'] = (1 + benchmark['benchmark_return']).cumprod()
    
    # 合并
    result = daily_pnl.join(benchmark, how='inner')
    return result

def performance_metrics(returns_series, annualization=252):
    """计算策略绩效指标"""
    ret = returns_series.dropna()
    if len(ret) < 10:
        return {}
    
    cum_ret = (1 + ret).prod() - 1
    annual_ret = (1 + cum_ret) ** (annualization / len(ret)) - 1
    vol = ret.std() * np.sqrt(annualization)
    sharpe = annual_ret / vol if vol != 0 else np.nan
    max_dd = calculate_max_drawdown((1 + ret).cumprod())
    
    return {
        '总收益': f"{cum_ret:.2%}",
        '年化收益': f"{annual_ret:.2%}",
        '年化波动率': f"{vol:.2%}",
        '夏普比率': f"{sharpe:.2f}",
        '最大回撤': f"{max_dd:.2%}"
    }

def calculate_max_drawdown(cum_return_series):
    """计算最大回撤"""
    rolling_max = cum_return_series.expanding().max()
    drawdown = (cum_return_series - rolling_max) / rolling_max
    return drawdown.min()

def plot_backtest_result(result_df):
    """绘制回测结果图"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(result_df.index, result_df['cum_return'], label='动量策略', color='tab:red')
    ax.plot(result_df.index, result_df['cum_benchmark'], label='等权基准', color='tab:blue', linestyle='--')
    
    ax.set_title('动量因子策略 vs 等权基准（累计净值）', fontsize=14)
    ax.set_ylabel('累计净值')
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "backtest_performance.png"), dpi=150, bbox_inches='tight')
    plt.close()

def main():
    print("🔄 开始回测动量因子策略...")
    
    # 1. 加载因子数据
    panel = load_factor_panel()
    print(f"加载 {panel['symbol'].nunique()} 只股票的因子数据")
    
    # 2. 生成信号
    panel = generate_signals(panel, top_n=TOP_N)
    print(f"✅ 生成每日持仓信号（做多动量前{TOP_N}只）")
    
    # 3. 计算组合收益
    result = calculate_portfolio_returns(panel)
    print(f"📊 回测区间: {result.index.min().date()} 至 {result.index.max().date()}")
    
    # 4. 绩效分析
    strategy_metrics = performance_metrics(result['strategy_return'])
    benchmark_metrics = performance_metrics(result['benchmark_return'])
    
    print("\n" + "="*50)
    print("📈 策略绩效报告")
    print("="*50)
    print("【动量策略】")
    for k, v in strategy_metrics.items():
        print(f"  {k}: {v}")
    
    print("\n【等权基准】")
    for k, v in benchmark_metrics.items():
        print(f"  {k}: {v}")
    
    # 5. 可视化
    plot_backtest_result(result)
    print(f"\n✅ 回测完成！净值曲线已保存至: {os.path.abspath(os.path.join(OUTPUT_DIR, 'backtest_performance.png'))}")
    
    # 6. 保存结果
    result.to_csv(os.path.join(OUTPUT_DIR, "backtest_result.csv"))
    panel[['datetime', 'symbol', 'mom_factor', 'position']].to_csv(
        os.path.join(OUTPUT_DIR, "positions.csv"), index=False
    )
    print("📁 已保存回测结果与持仓明细")

if __name__ == "__main__":
    main()