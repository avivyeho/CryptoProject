import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from configurations import *
from createDB import create_database

# Function to make an asynchronous HTTP request
async def make_request(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.json()
    except Exception as e:
        print(f"Error making request: {e}")
        return None

# Function to process the KuCoin API response
def process_kucoin_response(response):
    try:
        data = response.get('data', {})

        # Extract bid and ask prices
        kucoin_bid_price = float(data.get('bestBid', 0))
        kucoin_ask_price = float(data.get('bestAsk', 0))

        absolute = kucoin_ask_price - kucoin_bid_price
        spread = (absolute) / kucoin_ask_price
        spread_percentage = round(spread * 100, 6)

        slippage = ((kucoin_ask_price + 0.02 * absolute) - kucoin_ask_price) / kucoin_ask_price
        slippage_percentage = round(slippage * 100, 6)

        return kucoin_bid_price, kucoin_ask_price, spread_percentage, slippage_percentage
    except Exception as e:
        print(f'Error processing KuCoin response: {e}')
        return None, None, None, None

# Function to process the Binance API response
def process_binance_response(response):
    try:
        # Extract bid and ask prices
        binance_bid_price = float(response.get('bidPrice', 0))
        binance_ask_price = float(response.get('askPrice', 0))

        absolute = binance_ask_price - binance_bid_price
        spread = (absolute) / binance_ask_price
        spread_percentage = round(spread * 100, 6)

        slippage = ((binance_ask_price + 0.02 * absolute) - binance_ask_price) / binance_ask_price
        slippage_percentage = round(slippage * 100, 6)

        return binance_bid_price, binance_ask_price, spread_percentage, slippage_percentage
    except Exception as e:
        print(f'Error processing Binance response: {e}')
        return None, None, None , None

# Function to store data in SQLite
def store_data_in_sqlite(timestamp, market, kucoin_bid_price, kucoin_ask_price, kucoin_spread_percentage, kucoin_slippage_percentage, binance_bid_price, binance_ask_price, binance_spread_percentage, binance_slippage_percentage):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # SQL query to insert data into the table
    insert_data_query = '''
        INSERT INTO market_data (timestamp, market, kucoin_bid_price, kucoin_ask_price, kucoin_spread_percentage, kucoin_slippage_percentage, binance_bid_price, binance_ask_price, binance_spread_percentage, binance_slippage_percentage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    data = [
        (timestamp, market, kucoin_bid_price, kucoin_ask_price, kucoin_spread_percentage, kucoin_slippage_percentage, binance_bid_price, binance_ask_price, binance_spread_percentage, binance_slippage_percentage)
    ]

    # Insert data into the table
    cursor.executemany(insert_data_query, data)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Data has been successfully stored in SQLite.")

# Function to make an asynchronous KuCoin API request
async def API_request_Kucoin(symbol):
    kucoin_url = 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}'.format(symbol)
    kucoin_response = await make_request(kucoin_url)
    return kucoin_response

# Function to make an asynchronous Binance API request
async def API_request_Binance(symbol):
    binance_url = 'https://api.binance.com/api/v3/ticker/bookTicker?symbol={}'.format(symbol)
    binance_response = await make_request(binance_url)
    return binance_response

# Function to create responses and update the database
async def create_responses():
    while True:
        try:
            # Create a list of requests for KuCoin and Binance
            kucoin_requests = [API_request_Kucoin(symbol) for symbol in KUCOIN_MARKET]
            binance_requests = [API_request_Binance(symbol) for symbol in BINANCE_MARKET]

            # Gather the requests from KuCoin and Binance
            kucoin_responses = await asyncio.gather(*kucoin_requests)
            binance_responses = await asyncio.gather(*binance_requests)

            # Process and store the data
            for kucoin_response, binance_response, market in zip(kucoin_responses, binance_responses, KUCOIN_MARKET):
                kucoin_bid_price, kucoin_ask_price, kucoin_spread_percentage, kucoin_slippage_percentage = process_kucoin_response(kucoin_response)
                binance_bid_price, binance_ask_price, binance_spread_percentage, binance_slippage_percentage = process_binance_response(binance_response)

                if kucoin_bid_price is not None and kucoin_ask_price is not None and binance_bid_price is not None and binance_ask_price is not None:
                    timestamp = datetime.now()
                    store_data_in_sqlite(timestamp, market, kucoin_bid_price, kucoin_ask_price, kucoin_spread_percentage, kucoin_slippage_percentage, binance_bid_price, binance_ask_price, binance_spread_percentage, binance_slippage_percentage)

        except Exception as e:
            print(f"Error in create_responses: {e}")

        await asyncio.sleep(20)


def main():
    # Create the SQLite database and table if they don't exist
    create_database()
    asyncio.run(create_responses())

if __name__ == "__main__":
    main()
