import streamlit as st
import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

scheduler = BackgroundScheduler()

def price_format(val):
    if isinstance(val, (int, float)):  # Check if val is a number
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
    return "N/A"  # Return "N/A" for non-numeric values

@st.cache_data(ttl=86400)  # Cache the data for 24 hours (86400 seconds)
def get_top_500_usdt_pairs_by_volume():
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/24hr')
        tickers = response.json()

        # Print the actual API response for debugging
        print(f"API response: {tickers}")
        
        if isinstance(tickers, list):
            # Ensure that every item in the list is a dictionary
            if all(isinstance(item, dict) for item in tickers):
                df = pd.DataFrame(tickers)
                df = df[df['symbol'].str.endswith('USDT')]
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                top_500_usdt_pairs = df.sort_values('volume', ascending=False).head(500)
                st.write(top_500_usdt_pairs['symbol'].tolist())
                return top_500_usdt_pairs['symbol'].tolist()
            else:
                st.write("Error: API response does not contain a list of dictionaries.")
                return []
        else:
            st.write("Error: Unexpected API response format.")
            return []
    except Exception as e:
        st.write(f"Error fetching top USDT pairs: {e}")
        return []
        
@st.cache_data(ttl=86400)  # Cache the historical data for 24 hours
def get_historical_data(symbol, interval='1d', limit=500):
    try:
        url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
        response = requests.get(url)
        klines = response.json()
        print(f"Raw response for {symbol}: {klines}")  # Debug log
        
        # Check if the response is valid
        if isinstance(klines, list) and len(klines) > 0:
            df = pd.DataFrame(klines, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 
                                                'close_time', 'quote_asset_volume', 'number_of_trades', 
                                                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 
                                                'ignore'])
            df['close'] = pd.to_numeric(df['close'])
            df['time'] = pd.to_datetime(df['close_time'], unit='ms')
            return df
        else:
            print(f"Error: Unexpected API response structure for {symbol}")
            return pd.DataFrame()  # Return empty DataFrame on failure
    except Exception as e:
        print(f"Error fetching historical data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_ema_and_breakout(df, period=34):
    df = df.copy()
    df['EMA_34'] = df['close'].ewm(span=period, adjust=False).mean()  # Use pandas' built-in EMA
    df = df.dropna(subset=['close', 'EMA_34'])
    df['difference_34'] = (df['close'] - df['EMA_34']) / df['EMA_34'] * 100
    df['difference_34'] = df['difference_34'].apply(lambda x: f"{x:.2f}%")
    df['status_34'] = df.apply(lambda row: 'breakup' if row['close'] > row['EMA_34'] else 'breakdown', axis=1)
    return df

def calculate_ema102_and_breakout(df, period=102):
    df = df.copy()
    df['EMA_102'] = df['close'].ewm(span=period, adjust=False).mean()  # Use pandas' built-in EMA
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
        print(f"Fetching historical data for: {symbol}")  # Debug log
        df = get_historical_data(symbol, limit=500)
        if df.empty:
            print(f"No data for symbol: {symbol}")  # Debug log
            continue
        
        df_ema34 = calculate_ema_and_breakout(df)
        df_ema102 = calculate_ema102_and_breakout(df)

        if not df_ema34.empty and not df_ema102.empty:
            latest_34 = df_ema34.iloc[-1]
            latest_102 = df_ema102.iloc[-1]
            if pd.notna(latest_34['EMA_34']) and pd.notna(latest_102['EMA_102']):
                print(f"Adding data for: {symbol}")  # Debug log
                coins_data.append([
                    symbol,
                    latest_34['close'],
                    latest_34['status_34'],
                    latest_34['difference_34'],
                    latest_102['EMA_102'],
                    latest_102['status_102'],
                    latest_102['difference_102']
                ])

    # Check if coins_data is empty before creating DataFrame
    if not coins_data:
        print("No valid data collected.")  # Debug log
        return pd.DataFrame()  # Return an empty DataFrame if no data is collected

    # Create DataFrame with capitalized headings
    result_df = pd.DataFrame(coins_data, columns=['COIN', 'CLOSE', 'STATUS34', '%DIFF34', 
                                                  'LINE102', 'STATUS102', '%DIFF102'])
    print(f"Result DataFrame shape: {result_df.shape}")  # Debug log

    # Apply price formatting for CLOSE and LINE102 columns
    result_df['CLOSE'] = result_df['CLOSE'].apply(price_format)
    result_df['LINE102'] = result_df['LINE102'].apply(price_format)
    
    return result_df

def main():
    st.title("EMA Breakup Data")

    # Create container with border using custom CSS
    container = st.container()

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

    with container:
        st.write("**This data is from Binance filtered by top 500 coins by volume and shows only USDT.**")
        st.write("**The data provides a view of whether the price is above or below EMA of 34 days and EMA of 102 days, as well as the percentage difference after breakup/down.**")
        st.write("**Why 102 days? It is roughly equal to the 3-day EMA, which usually differs by only around 5%. This makes calculations easier.**")

    # Display data with a spinner while fetching
    with st.spinner("Fetching and processing data..."):
        result_df = fetch_and_process_data()  # Fetch and cache the data for the day

    if result_df is not None and not result_df.empty:
        breakup_df = result_df[result_df['STATUS34'] == 'breakup']
        sorted_breakup_df = breakup_df.sort_values(by='%DIFF34')

        # Display the sorted dataframe
        st.dataframe(sorted_breakup_df)
    else:
        st.write("No data available. Please try again later.")

if __name__ == "__main__":
    scheduler.start()
    main()

    



