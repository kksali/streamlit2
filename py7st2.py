import streamlit as st
import pandas as pd
import ccxt
from datetime import datetime

# Initialize ccxt Binance client
binance = ccxt.binance()

def price_format(val):
    if isinstance(val, (int, float)):
        if val > 0.1:
            return '{:,.2f}'.format(val)
        elif val > 0.01:
            return '{:,.4f}'.format(val)
        elif val > 0.0001:
            return '{:,.6f}'.format(val)
        elif val > 0.000001:
            return '{:,.8f}'.format(val)
        elif val > 0.00000001:
            return '{:,.10f}'.format(val)
        elif val > 0.0000000001:
            return '{:,.12f}'.format(val)
        else:
            return '{:,.15f}'.format(val)
    return "N/A"

@st.cache_data(ttl=86400)  # Cache the data for 24 hours (86400 seconds)
def get_top_500_usdt_pairs_by_volume():
    try:
        tickers = binance.fetch_tickers()
        tickers_df = pd.DataFrame(tickers).transpose()
        tickers_df = tickers_df[tickers_df['symbol'].str.endswith('USDT')]
        tickers_df['volume'] = pd.to_numeric(tickers_df['quoteVolume'], errors='coerce')
        top_500_usdt_pairs = tickers_df.sort_values('volume', ascending=False).head(500)
        return top_500_usdt_pairs['symbol'].tolist()
    except Exception as e:
        print(f"Error fetching top USDT pairs: {e}")
        return []

@st.cache_data(ttl=86400)  # Cache the historical data for 24 hours
def get_historical_data(symbol, interval='1d', limit=500):
    try:
        ohlcv = binance.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
        df['close'] = pd.to_numeric(df['close'])
        df['time'] = pd.to_datetime(df['open_time'], unit='ms')
        return df
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_ema_and_breakout(df, period=34):
    df = df.copy()
    df['EMA_34'] = df['close'].ewm(span=period, adjust=False).mean()
    df = df.dropna(subset=['close', 'EMA_34'])
    df['difference_34'] = (df['close'] - df['EMA_34']) / df['EMA_34'] * 100
    df['difference_34'] = df['difference_34'].apply(lambda x: f"{x:.2f}%")
    df['status_34'] = df.apply(lambda row: 'breakup' if row['close'] > row['EMA_34'] else 'breakdown', axis=1)
    return df

def calculate_ema102_and_breakout(df, period=102):
    df = df.copy()
    df['EMA_102'] = df['close'].ewm(span=period, adjust=False).mean()
    df = df.dropna(subset=['close', 'EMA_102'])
    df['difference_102'] = (df['close'] - df['EMA_102']) / df['EMA_102'] * 100
    df['difference_102'] = df['difference_102'].apply(lambda x: f"{x:.2f}%")
    df['status_102'] = df.apply(lambda row: 'breakup' if row['close'] > row['EMA_102'] else 'breakdown', axis=1)
    return df

@st.cache_data(ttl=86400)  # Cache the processed data for 24 hours
def fetch_and_process_data():
    top_usdt_pairs = get_top_500_usdt_pairs_by_volume()
    coins_data = []

    for symbol in top_usdt_pairs:
        df = get_historical_data(symbol, limit=500)
        if df.empty:
            continue
        df_ema34 = calculate_ema_and_breakout(df)
        df_ema102 = calculate_ema102_and_breakout(df)

        if not df_ema34.empty and not df_ema102.empty:
            latest_34 = df_ema34.iloc[-1]
            latest_102 = df_ema102.iloc[-1]
            if pd.notna(latest_34['EMA_34']) and pd.notna(latest_102['EMA_102']):
                coins_data.append([symbol, latest_34['close'], latest_34['status_34'], latest_34['difference_34'],
                                   latest_102['EMA_102'], latest_102['status_102'], latest_102['difference_102']])

    result_df = pd.DataFrame(coins_data, columns=['COIN', 'CLOSE', 'STATUS34', '%DIFF34', 
                                                  'LINE102', 'STATUS102', '%DIFF102'])
    
    # Apply price formatting
    result_df['CLOSE'] = result_df['CLOSE'].apply(price_format)
    result_df['LINE102'] = result_df['LINE102'].apply(price_format)
    
    return result_df

def main():
    st.title("EMA Breakup Data")

    # Add custom CSS for border
    st.markdown(
        """
        <style>
        .container {
            border: 2px solid #4CAF50;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    st.write("**This data is from Binance using `ccxt`, filtered by top 500 coins by volume and shows only USDT pairs.**")
    st.write("**It provides a view of whether the price is above or below EMA of 34 days and EMA of 102 days, along with the percentage difference.**")

    # Display data with spinner
    with st.spinner("Fetching and processing data..."):
        result_df = fetch_and_process_data()

    if result_df is not None and not result_df.empty:
        breakup_df = result_df[result_df['STATUS34'] == 'breakup']
        sorted_breakup_df = breakup_df.sort_values(by='%DIFF34')

        st.dataframe(sorted_breakup_df)
    else:
        st.write("No data available. Please try again later.")

if __name__ == "__main__":
    main()

