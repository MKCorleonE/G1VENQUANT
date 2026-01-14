# coding=utf8
"""
Created on Thu Sep 5 11:02:00 2025
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import tushare as ts
import pandas as pd
from tqdm import tqdm

# ======= 配置区 =======
TOKEN = "9bb81649792cc92d8e0ed2a5789d47b4bcd74a53b224ce44f3a4e0e6"  # pro版本token
DATA_DIR = "./data/tushare_selected_stocks"  # 数据保存目录
START_DATE = "20150101" #起始日期
END_DATE = "20251231" #结束日期

# 🔴 在这里指定需要下载的股票（使用 Tushare 的 ts_code 格式）
# 格式：'股票代码.交易所'，如 '000001.SZ'（平安银行）、'600519.SH'（贵州茅台）
SELECTED_STOCKS = [
    '000001.SZ',  # 平安银行
    '600519.SH',  # 贵州茅台
    '300750.SZ',  # 宁德时代
    '000858.SZ',  # 五粮液
    '601318.SH',  # 中国平安
    '000333.SZ',  # 美的集团
    # 可以在这里继续添加
]

# 创建数据存储目录
os.makedirs(DATA_DIR, exist_ok=True)

# 初始化Tushare接口
ts.set_token(TOKEN)
pro = ts.pro_api()

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

# ======= 主程序 ========
def main():
    total = len(SELECTED_STOCKS)
    print(f"准备下载 {total} 只指定股票的数据...")
    print("股票列表:", SELECTED_STOCKS)

    success_count = 0
    # 使用 tqdm 显示进度条
    for ts_code in tqdm(SELECTED_STOCKS, desc="Downloading"):
        symbol = ts_code.split('.')[0]
        file_path = os.path.join(DATA_DIR, f"{symbol}.csv")

        # 跳过已存在的文件（断点续传）
        if os.path.exists(file_path):
            continue

        if download_stock_data(ts_code, START_DATE, END_DATE):
            success_count += 1

    print(f"\n✅ 下载完成！成功: {success_count}/{total} 只股票")
    print(f"数据保存在: {os.path.abspath(DATA_DIR)}")

if __name__ == "__main__":
    main()