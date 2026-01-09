# src/crypto_download.py
"""
下载 OKX 加密货币 K 线数据（日线/小时线）
支持 BTC/USDT, ETH/USDT 等主流交易对
作者: 梁嘉文
项目: G1VENQUANT
"""

import os
import ccxt
import pandas as pd
from datetime import datetime, timezone

# ========== 配置区 ==========
SYMBOLS = ["BTC/USDT"]  # 交易对（OKX 格式）
TIMEFRAME = "1d"                   # K线周期: 1m, 5m, 15m, 1h, 4h, 1d, 1w
START_DATE = "2024-01-01T00:00:00Z"
SAVE_TO_LOCAL = True
DATA_DIR = "data/raw"

# ========== 主逻辑 ==========
def fetch_ohlcv(symbol: str, timeframe: str, since: str) -> pd.DataFrame:
    """
    从 OKX 下载 OHLCV 数据
    :param symbol: 交易对，如 "BTC/USDT"
    :param timeframe: K线周期
    :param since: 起始时间 (ISO 8601)
    """
    try:
        # 初始化 OKX 交易所（公开访问，无需 API key）
        exchange = ccxt.okx({
            'enableRateLimit': True,  # 自动遵守速率限制
            'options': {'defaultType': 'spot'}  # 现货交易
        })

        # 转换起始时间为毫秒时间戳
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        since_ts = int(since_dt.timestamp() * 1000)

        all_ohlcv = []
        limit = 100  # 每次最多 100 根 K 线（OKX 限制）

        print(f"📥 开始下载 {symbol} {timeframe} 数据...")
        while True:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since_ts, limit=limit)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            since_ts = ohlcv[-1][0] + 1  # 下一页起始时间
            print(f"  已获取 {len(all_ohlcv)} 根 K 线...", end="\r")

            # 防止请求过快（ccxt 已内置 rate limit，但加 sleep 更安全）
            import time
            time.sleep(0.1)

        if not all_ohlcv:
            return pd.DataFrame()

        # 转为 DataFrame
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('datetime', inplace=True)

        # 保留所需列
        df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        return df

    except Exception as e:
        print(f"❌ 下载 {symbol} 失败: {e}")
        return pd.DataFrame()

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    for symbol in SYMBOLS:
        df = fetch_ohlcv(symbol, TIMEFRAME, START_DATE)
        if not df.empty:
            print(f"\n✅ {symbol} 下载成功，共 {len(df)} 条记录")
            if SAVE_TO_LOCAL:
                # 文件名标准化：BTC-USDT.csv
                filename = symbol.replace("/", "-") + ".csv"
                filepath = os.path.join(DATA_DIR, filename)
                df.to_csv(filepath)
                print(f"💾 已保存至 {filepath}")
            else:
                print(df.head())
        else:
            print(f"\n⚠️  {symbol} 无数据")

if __name__ == "__main__":
    main()