# scripts/fetch_data.py
"""
数据获取脚本 - 使用 AkShare 下载 A 股日线数据
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import akshare as ak
import pandas as pd
from datetime import datetime

# ========== 配置区 ==========
STOCK_CODES = ["000001", "600519", "300750"]  # 示例股票代码（平安银行、茅台、宁德时代）
START_DATE = "20150101"
END_DATE = datetime.today().strftime("%Y%m%d")
SAVE_TO_LOCAL = True  # ← 设置为 True 则保存 CSV 到本地；False 则仅打印
DATA_DIR = "../data/raw"

# ========== 主逻辑 ==========
def fetch_stock_daily(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """获取单只股票的日线数据"""
    try:
        # AkShare 的股票后缀规则：沪市加 .SH，深市加 .SZ
        if stock_code.startswith("6"):
            symbol = f"{stock_code}.SH"
        else:
            symbol = f"{stock_code}.SZ"
        
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="qfq"  # 前复权
        )
        df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount'
        }, inplace=True)
        df['code'] = stock_code
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df[['code', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    except Exception as e:
        print(f"❌ 获取 {stock_code} 失败: {e}")
        return pd.DataFrame()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    for code in STOCK_CODES:
        print(f"📥 正在获取 {code} 的数据...")
        df = fetch_stock_daily(code, START_DATE, END_DATE)
        
        if not df.empty:
            print(f"✅ 获取 {code} 成功，共 {len(df)} 条记录")
            
            # ========== 保存到本地（通过开关控制）==========
            if SAVE_TO_LOCAL:
                filepath = os.path.join(DATA_DIR, f"{code}.csv")
                df.to_csv(filepath)
                print(f"💾 已保存至 {filepath}")
            else:
                print(df.head(3))  # 仅预览
        else:
            print(f"⚠️  {code} 无有效数据")

if __name__ == "__main__":
    main()