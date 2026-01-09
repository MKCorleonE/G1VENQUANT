# src/data_download.py
"""
数据获取脚本 - 使用 AkShare 下载 A 股日线数据（适配 v1.10+）
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import akshare as ak
import pandas as pd
from datetime import datetime

# ========== 配置区 ==========
STOCK_CODES = ["000001", "600519", "300750"]  # 平安银行、贵州茅台、宁德时代
START_DATE = "20150101"
END_DATE = datetime.today().strftime("%Y%m%d")
SAVE_TO_LOCAL = True
DATA_DIR = "data/raw"  # 注意：相对路径，确保 data/raw 存在

# ========== 主逻辑 ==========
def fetch_stock_daily(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """获取单只股票的日线数据（前复权）"""
    try:
        # 自动判断市场
        if stock_code.startswith(("6", "9")):  # 沪市：60/68/90 开头
            symbol = f"{stock_code}.SH"
        else:  # 深市：00/30 开头
            symbol = f"{stock_code}.SZ"
        
        # 调用 AkShare 接口（新版返回英文列名）
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="qfq"  # 前复权
        )
        
        if df.empty:
            return df
        
        # 新版 AkShare 已返回英文列名，无需重命名
        # 但为保险起见，可统一列名（防止未来变动）
        expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        if not all(col in df.columns for col in expected_cols):
            print(f"⚠️  {stock_code} 返回列不匹配: {df.columns.tolist()}")
            return pd.DataFrame()
        
        df = df[expected_cols].copy()
        df['code'] = stock_code
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df

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
            
            if SAVE_TO_LOCAL:
                filepath = os.path.join(DATA_DIR, f"{code}.csv")
                df.to_csv(filepath)
                print(f"💾 已保存至 {filepath}")
            else:
                print(df.head(3))
        else:
            print(f"⚠️  {code} 无有效数据")

if __name__ == "__main__":
    main()