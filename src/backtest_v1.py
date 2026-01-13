import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class MomentumStrategyBacktest:
    """动量策略回测类"""
    
    def __init__(self, panel_data, initial_capital=1000000, 
                 transaction_cost=0.001, lookback_period=20):
        """
        初始化回测参数
        
        参数:
        ----------
        panel_data : DataFrame
            包含['datetime', 'symbol', 'close', 'volume', 'future_return']的面板数据
        initial_capital : float
            初始资金
        transaction_cost : float
            交易成本（单边，如0.001表示0.1%）
        lookback_period : int
            动量计算周期
        """
        self.panel = panel_data.copy()
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.lookback = lookback_period
        self.results = {}
        
    def prepare_data(self):
        """准备回测数据"""
        print("准备回测数据...")
        
        # 1. 确保数据已按时间和股票排序
        self.panel = self.panel.sort_values(['symbol', 'datetime']).reset_index(drop=True)
        
        # 2. 计算动量因子
        self.panel['momentum'] = self.panel.groupby('symbol')['close'].transform(
            lambda x: x.pct_change(periods=self.lookback)
        )
        
        # 3. 计算未来收益率（用于验证）
        self.panel['next_return'] = self.panel.groupby('symbol')['close'].transform(
            lambda x: x.pct_change().shift(-1)
        )
        
        # 4. 计算市值（假设有市值数据，如果没有可以简单用价格*成交量估算）
        if 'market_cap' not in self.panel.columns:
            # 简单估算：价格 * 成交量 * 一个系数
            self.panel['market_cap'] = self.panel['close'] * self.panel['volume'] * 0.001
        
        # 5. 添加日期标记
        self.panel['date'] = pd.to_datetime(self.panel['datetime'])
        self.panel['year'] = self.panel['date'].dt.year
        self.panel['month'] = self.panel['date'].dt.month
        
        print(f"数据期间: {self.panel['date'].min()} 到 {self.panel['date'].max()}")
        print(f"股票数量: {self.panel['symbol'].nunique()}")
        print(f"总数据点: {len(self.panel)}")
        
    def generate_signals(self, top_n=10, long_only=True):
        """生成交易信号
        
        参数:
        ----------
        top_n : int
            买入/卖出的股票数量
        long_only : bool
            True: 只做多, False: 多空策略
        """
        print("生成交易信号...")
        
        signals = []
        
        # 按天生成信号
        dates = sorted(self.panel['date'].dropna().unique())
        
        for date in dates:
            # 获取当天的数据
            daily_data = self.panel[self.panel['date'] == date].copy()
            
            if len(daily_data) < top_n * 2:  # 至少需要2*top_n只股票
                continue
            
            # 去除缺失值
            daily_data = daily_data.dropna(subset=['momentum', 'next_return'])
            
            if len(daily_data) < 10:  # 股票太少
                continue
            
            # 按动量排序
            daily_data = daily_data.sort_values('momentum', ascending=False)
            
            # 生成信号
            long_stocks = daily_data.head(top_n)['symbol'].tolist()  # 动量最强的top_n只
            short_stocks = daily_data.tail(top_n)['symbol'].tolist()  # 动量最弱的top_n只
            
            signals.append({
                'date': date,
                'long_stocks': long_stocks,
                'short_stocks': short_stocks,
                'n_stocks': len(daily_data),
                'avg_momentum_long': daily_data.head(top_n)['momentum'].mean(),
                'avg_momentum_short': daily_data.tail(top_n)['momentum'].mean()
            })
        
        self.signals_df = pd.DataFrame(signals)
        print(f"生成 {len(self.signals_df)} 个交易日的信号")
        
        return self.signals_df
    
    def backtest_portfolio(self, rebalance_days=1, equal_weight=True):
        """执行回测
        
        参数:
        ----------
        rebalance_days : int
            再平衡天数（1=每天调仓）
        equal_weight : bool
            True: 等权重, False: 按市值加权
        """
        print("执行回测...")
        
        if not hasattr(self, 'signals_df'):
            raise ValueError("请先生成交易信号")
        
        # 准备结果存储
        portfolio_values = [self.initial_capital]
        returns = [0]
        dates = []
        positions_history = []
        
        current_capital = self.initial_capital
        current_positions = {}  # {symbol: shares}
        trade_log = []
        
        # 获取所有交易日
        trading_dates = sorted(self.panel['date'].unique())
        
        for i, date in enumerate(trading_dates):
            if i % rebalance_days != 0 and i != 0:
                # 非调仓日，只更新市值
                if current_positions:
                    # 计算当前持仓市值
                    today_prices = self.panel[self.panel['date'] == date].set_index('symbol')['close']
                    portfolio_value = sum(shares * today_prices.get(symbol, 0) 
                                         for symbol, shares in current_positions.items())
                    portfolio_value += current_capital  # 加现金
                    
                    # 计算当日收益率
                    daily_return = (portfolio_value - portfolio_values[-1]) / portfolio_values[-1]
                    returns.append(daily_return)
                    portfolio_values.append(portfolio_value)
                    dates.append(date)
                continue
            
            # === 调仓日 ===
            
            # 1. 获取当天的信号
            signal_today = self.signals_df[self.signals_df['date'] == date]
            
            if signal_today.empty:
                # 无信号，保持仓位
                if current_positions:
                    today_prices = self.panel[self.panel['date'] == date].set_index('symbol')['close']
                    portfolio_value = sum(shares * today_prices.get(symbol, 0) 
                                         for symbol, shares in current_positions.items())
                    portfolio_value += current_capital
                else:
                    portfolio_value = current_capital
                
                if i > 0:
                    daily_return = (portfolio_value - portfolio_values[-1]) / portfolio_values[-1]
                else:
                    daily_return = 0
                    
                returns.append(daily_return)
                portfolio_values.append(portfolio_value)
                dates.append(date)
                continue
            
            # 2. 平掉现有仓位
            if current_positions:
                # 计算平仓收益
                today_prices = self.panel[self.panel['date'] == date].set_index('symbol')['close']
                
                for symbol, shares in current_positions.items():
                    if symbol in today_prices.index:
                        price = today_prices[symbol]
                        sale_value = shares * price
                        sale_value_after_cost = sale_value * (1 - self.transaction_cost)  # 卖出成本
                        current_capital += sale_value_after_cost
                        
                        trade_log.append({
                            'date': date,
                            'action': 'SELL',
                            'symbol': symbol,
                            'shares': shares,
                            'price': price,
                            'value': sale_value,
                            'cost': sale_value * self.transaction_cost
                        })
                
                current_positions.clear()
            
            # 3. 建立新仓位
            signal = signal_today.iloc[0]
            long_stocks = signal['long_stocks']
            
            if not long_stocks:
                # 无股票可买
                daily_return = 0
                returns.append(daily_return)
                portfolio_values.append(current_capital)
                dates.append(date)
                continue
            
            # 获取今天的价格
            today_data = self.panel[self.panel['date'] == date]
            
            # 计算每只股票的权重
            if equal_weight:
                # 等权重
                weight_per_stock = 1.0 / len(long_stocks)
            else:
                # 按市值加权
                market_caps = today_data[today_data['symbol'].isin(long_stocks)].set_index('symbol')['market_cap']
                total_market_cap = market_caps.sum()
                weights = market_caps / total_market_cap
                weight_per_stock = dict(zip(weights.index, weights.values))
            
            # 买入股票
            for symbol in long_stocks:
                stock_data = today_data[today_data['symbol'] == symbol]
                if stock_data.empty:
                    continue
                
                price = stock_data.iloc[0]['close']
                
                if equal_weight:
                    weight = weight_per_stock
                else:
                    weight = weight_per_stock.get(symbol, 0)
                
                if weight <= 0:
                    continue
                
                # 计算买入金额
                invest_amount = current_capital * weight
                
                # 考虑买入成本
                invest_amount_after_cost = invest_amount * (1 - self.transaction_cost)
                shares_to_buy = invest_amount_after_cost / price
                
                if shares_to_buy > 0:
                    current_positions[symbol] = current_positions.get(symbol, 0) + shares_to_buy
                    current_capital -= invest_amount
                    
                    trade_log.append({
                        'date': date,
                        'action': 'BUY',
                        'symbol': symbol,
                        'shares': shares_to_buy,
                        'price': price,
                        'value': invest_amount,
                        'cost': invest_amount * self.transaction_cost
                    })
            
            # 4. 计算当日收益
            # 持仓市值
            if current_positions:
                portfolio_value = sum(shares * today_prices.get(symbol, 0) 
                                     for symbol, shares in current_positions.items())
            else:
                portfolio_value = 0
            
            total_value = portfolio_value + current_capital
            
            if i > 0:
                daily_return = (total_value - portfolio_values[-1]) / portfolio_values[-1]
            else:
                daily_return = 0
            
            returns.append(daily_return)
            portfolio_values.append(total_value)
            dates.append(date)
            
            # 记录持仓
            positions_history.append({
                'date': date,
                'positions': current_positions.copy(),
                'portfolio_value': total_value,
                'cash': current_capital
            })
        
        # 存储结果
        self.returns_series = pd.Series(returns[1:], index=dates)  # 去掉第一个0
        self.portfolio_values = pd.Series(portfolio_values[1:], index=dates)
        self.trade_log = pd.DataFrame(trade_log)
        self.positions_history = positions_history
        
        print("回测完成！")
        return self.returns_series, self.portfolio_values
    
    def calculate_metrics(self, risk_free_rate=0.02):
        """计算回测指标"""
        print("计算回测指标...")
        
        if not hasattr(self, 'returns_series'):
            raise ValueError("请先执行回测")
        
        returns = self.returns_series
        n_days = len(returns)
        
        # 基本指标
        total_return = (self.portfolio_values.iloc[-1] / self.initial_capital) - 1
        annual_return = (1 + total_return) ** (252 / n_days) - 1
        
        # 波动率
        annual_volatility = returns.std() * np.sqrt(252)
        
        # 夏普比率
        excess_returns = returns - risk_free_rate / 252
        sharpe_ratio = excess_returns.mean() / returns.std() * np.sqrt(252)
        
        # 最大回撤
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.expanding().max()
        drawdown = (cum_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # 索提诺比率
        negative_returns = returns[returns < 0]
        downside_std = negative_returns.std() * np.sqrt(252) if len(negative_returns) > 0 else 0
        sortino_ratio = (annual_return - risk_free_rate) / downside_std if downside_std > 0 else 0
        
        # 胜率
        winning_days = (returns > 0).sum()
        win_rate = winning_days / n_days
        
        # Calmar比率
        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # 交易统计
        n_trades = len(self.trade_log) if hasattr(self, 'trade_log') else 0
        avg_trade_return = returns.mean() * 252  # 年均化
        
        metrics = {
            '初始资金': self.initial_capital,
            '最终资金': self.portfolio_values.iloc[-1],
            '总收益率': total_return,
            '年化收益率': annual_return,
            '年化波动率': annual_volatility,
            '夏普比率': sharpe_ratio,
            '最大回撤': max_drawdown,
            '索提诺比率': sortino_ratio,
            '胜率': win_rate,
            'Calmar比率': calmar_ratio,
            '交易次数': n_trades,
            '平均交易收益': avg_trade_return,
            '交易天数': n_days
        }
        
        self.metrics = metrics
        return metrics
    
    def plot_results(self):
        """绘制回测结果图"""
        fig, axes = plt.subplots(3, 2, figsize=(15, 12))
        
        # 1. 净值曲线
        axes[0, 0].plot(self.portfolio_values.index, self.portfolio_values / self.initial_capital, 
                       label=f'策略净值 (总收益: {self.metrics["总收益率"]:.2%})', linewidth=2)
        axes[0, 0].set_title('策略净值曲线')
        axes[0, 0].set_ylabel('净值')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # 2. 收益率分布
        axes[0, 1].hist(self.returns_series * 100, bins=50, edgecolor='black', alpha=0.7)
        axes[0, 1].axvline(x=self.returns_series.mean() * 100, color='r', linestyle='--', 
                          label=f'均值: {self.returns_series.mean()*100:.2f}%')
        axes[0, 1].set_title('日收益率分布')
        axes[0, 1].set_xlabel('日收益率 (%)')
        axes[0, 1].set_ylabel('频数')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. 滚动夏普比率（20日窗口）
        rolling_sharpe = self.returns_series.rolling(window=20).mean() / \
                        self.returns_series.rolling(window=20).std() * np.sqrt(252)
        axes[1, 0].plot(rolling_sharpe.index, rolling_sharpe, label='20日滚动夏普比率', color='orange')
        axes[1, 0].axhline(y=self.metrics['夏普比率'], color='r', linestyle='--', 
                          label=f'平均夏普: {self.metrics["夏普比率"]:.2f}')
        axes[1, 0].set_title('滚动夏普比率')
        axes[1, 0].set_ylabel('夏普比率')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        # 4. 最大回撤
        cum_returns = (1 + self.returns_series).cumprod()
        running_max = cum_returns.expanding().max()
        drawdown = (cum_returns - running_max) / running_max
        
        axes[1, 1].fill_between(drawdown.index, drawdown * 100, 0, 
                               color='red', alpha=0.3, label='回撤')
        axes[1, 1].plot(drawdown.index, drawdown * 100, color='red', linewidth=1)
        axes[1, 1].set_title(f'最大回撤: {self.metrics["最大回撤"]:.2%}')
        axes[1, 1].set_ylabel('回撤 (%)')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)
        
        # 5. 月度收益热力图
        monthly_returns = self.returns_series.resample('M').apply(
            lambda x: (1 + x).prod() - 1
        )
        monthly_returns_df = pd.DataFrame({
            'year': monthly_returns.index.year,
            'month': monthly_returns.index.month,
            'return': monthly_returns.values
        })
        monthly_pivot = monthly_returns_df.pivot(index='year', columns='month', values='return')
        
        im = axes[2, 0].imshow(monthly_pivot * 100, cmap='RdYlGn', aspect='auto')
        axes[2, 0].set_title('月度收益率热力图 (%)')
        axes[2, 0].set_xlabel('月份')
        axes[2, 0].set_ylabel('年份')
        plt.colorbar(im, ax=axes[2, 0])
        
        # 6. 关键指标表格
        metrics_text = f"""
        年化收益率: {self.metrics['年化收益率']:.2%}
        夏普比率: {self.metrics['夏普比率']:.2f}
        最大回撤: {self.metrics['最大回撤']:.2%}
        索提诺比率: {self.metrics['索提诺比率']:.2f}
        胜率: {self.metrics['胜率']:.2%}
        交易次数: {self.metrics['交易次数']}
        """
        axes[2, 1].text(0.1, 0.5, metrics_text, fontsize=12, verticalalignment='center',
                       bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
        axes[2, 1].set_title('关键指标')
        axes[2, 1].axis('off')
        
        plt.tight_layout()
        plt.show()
    
    def run_full_backtest(self, top_n=10, rebalance_days=1):
        """运行完整回测流程"""
        print("=" * 50)
        print("开始动量策略回测")
        print("=" * 50)
        
        # 1. 准备数据
        self.prepare_data()
        
        # 2. 生成信号
        self.generate_signals(top_n=top_n, long_only=True)
        
        # 3. 执行回测
        returns, portfolio_values = self.backtest_portfolio(rebalance_days=rebalance_days)
        
        # 4. 计算指标
        metrics = self.calculate_metrics()
        
        # 5. 打印结果
        print("\n" + "=" * 50)
        print("回测结果汇总")
        print("=" * 50)
        for key, value in metrics.items():
            if isinstance(value, float):
                if '率' in key or '收益' in key or '撤' in key:
                    print(f"{key}: {value:.2%}")
                else:
                    print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")
        
        # 6. 绘图
        self.plot_results()
        
        return metrics

# 使用示例
if __name__ == "__main__":
    # 假设 panel_data 是你的数据
    # panel_data 应包含: datetime, symbol, close, volume, future_return
    
    # 创建回测实例
    backtester = MomentumStrategyBacktest(
        panel_data=panel_data,  # 你的面板数据
        initial_capital=1000000,  # 初始资金100万
        transaction_cost=0.001,  # 交易成本0.1%
        lookback_period=20  # 20日动量
    )
    
    # 运行完整回测
    try:
        results = backtester.run_full_backtest(
            top_n=10,  # 买入前10只股票
            rebalance_days=5  # 每5天调仓一次
        )
    except Exception as e:
        print(f"回测过程中出错: {e}")
        raise