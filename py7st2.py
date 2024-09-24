import streamlit as st
import pandas as pd
import pandas_ta as ta
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import numpy as np  # Import NumPy

scheduler = BackgroundScheduler()

def get_top_500_usdt_pairs_by_volume():
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/24hr')
        tickers = response.json()
        df = pd.DataFrame(tickers)
        df = df[df['symbol'].str.endswith('USDT')]
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        top_500_usdt_pairs = df.sort_values('volume', ascending=False).head(500)
        return top_500_usdt_pairs['symbol'].tolist()
    except Exception as e:
        print(f"Error fetching top USDT pairs: {e}")
        return []

def get_historical_data(symbol, interval='1d', limit=500):
    try:
        url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
        response = requests.get(url)
        klines = response.json()
        df = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 
                                            'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 
                                            'taker_buy_quote_asset_volume', 'ignore'])
        df['close'] = pd.to_numeric(df['close'])
        df['time'] = pd.to_datetime(df['close_time'], unit='ms')
        return df
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_ema_and_breakout(df, period=34):
    df = df.copy()
    df['EMA_34'] = ta.ema(df['close'], length=period)
    
    # Drop rows with NaN values in 'close' or 'EMA_34'
    df.dropna(subset=['close', 'EMA_34'], inplace=True)
    
    df['difference_34'] = (df['close'] - df['EMA_34']) / df['EMA_34'] * 100
    df['difference_34'] = df['difference_34'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "NaN")
    df['status_34'] = df.apply(lambda row: 'breakup' if row['close'] > row['EMA_34'] else 'breakdown', axis=1)
    
    return df

def calculate_ema102_and_breakout(df, period=102):
    df = df.copy()
    df['EMA_102'] = ta.ema(df['close'], length=period)
    
    # Drop rows with NaN values in 'close' or 'EMA_102'
    df.dropna(subset=['close', 'EMA_102'], inplace=True)
    
    df['difference_102'] = (df['close'] - df['EMA_102']) / df['EMA_102'] * 100
    df['difference_102'] = df['difference_102'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "NaN")
    df['status_102'] = df.apply(lambda row: 'breakup' if row['close'] > row['EMA_102'] else 'breakdown', axis=1)
    
    return df

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

    result_df = pd.DataFrame(coins_data, columns=['Coin', 'Close Price', 'Status_34', 'Percentage Difference EMA_34', 
                                                  'EMA_102', 'Status_102', 'Percentage Difference EMA_102'])
    breakup_df = result_df[result_df['Status_34'] == 'breakup']
    sorted_breakup_df = breakup_df.sort_values(by='Percentage Difference EMA_34')

    print(f"Data fetched and processed at {datetime.utcnow()}")

scheduler.add_job(fetch_and_process_data, 'cron', hour=0, minute=1, timezone='UTC')

def main():
    st.title("EMA Breakup Data")

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

    result_df = pd.DataFrame(coins_data, columns=['Coin', 'Close Price', 'Status_34', 'Percentage Difference EMA_34', 
                                                  'EMA_102', 'Status_102', 'Percentage Difference EMA_102'])
    breakup_df = result_df[result_df['Status_34'] == 'breakup']
    sorted_breakup_df = breakup_df.sort_values(by='Percentage Difference EMA_34')

    st.dataframe(sorted_breakup_df)

if __name__ == "__main__":
    scheduler.start()
    main()

