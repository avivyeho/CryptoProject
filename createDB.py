import sqlite3
from configurations import DB_FILE


def create_database():
    """Create the SQLite database if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # SQL query to create the database and market_data table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            market TEXT,
            kucoin_bid_price REAL,
            kucoin_ask_price REAL,
            kucoin_spread_percentage REAL,
            kucoin_slippage_percentage REAL,
            binance_bid_price REAL,
            binance_ask_price REAL,
            binance_spread_percentage REAL,
            binance_slippage_percentage REAL
        )
    '''

    # Execute the query to create the table
    cursor.execute(create_table_query)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

