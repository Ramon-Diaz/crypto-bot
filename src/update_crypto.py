import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from time import sleep
import requests
from datetime import datetime, timedelta
import ccxt
import mysql.connector

class UpdateCrypto:
    """
    A class for updating cryptocurrency data from exchanges and inserting it into a MySQL database.
    """
    def __init__(self, currency) -> None:
        self.currency_ = currency

        # Retrieve secrets from environment variables
        self.db_user = os.getenv('MYSQL_USER')
        self.db_password = os.getenv('MYSQL_PASSWORD')
        self.db_host = os.getenv('MYSQL_HOST', 'mysql')
        self.db_name = os.getenv('MYSQL_DATABASE', 'crypto_data')

        # List of cryptocurrency trading pairs in Binance
        self.currencies_ = {
            'BTCUSD': 'BTC/USDT', 
            'ETHUSD': 'ETH/USDT', 
            'ETCUSD': 'ETC/USDT', 
            'XRPUSD': 'XRP/USDT', 
            'ADAUSD': 'ADA/USDT',
            'SOLUSD': 'SOL/USDT', 
            'DOGEUSD': 'DOGE/USDT', 
            'MATICUSD': 'MATIC/USDT', 
            'LINKUSD': 'LINK/USDT', 
            'LTCUSD': 'LTC/USDT'
        }

    def get_last_date(self) -> datetime:
        """
        Retrieves the latest date from a specified table in the MySQL database.

        Returns:
            datetime: The next date from which to start fetching data.
        """
        # Create a connection string
        connection_string = f'mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_name}'
        print(f"Connecting to MySQL database: {self.db_name}")
        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Create a configured "Session" class
        Session = sessionmaker(bind=engine)

        # Create a session
        session = Session()

        try:
            # SQL query to get the most recent datetime from the table
            query = text(f"SELECT datetime FROM {self.currency_} ORDER BY datetime DESC LIMIT 1;")
            result = session.execute(query)

            # Fetch the last date
            last_date = result.fetchall()[0][0]
            print(f"Last date: {last_date}")

        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            session.rollback()

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        finally:
            session.close()
            engine.dispose()

        request_date = last_date + timedelta(minutes=1)
        return request_date

    def get_data(self, request_date: datetime) -> pd.DataFrame:
        """
        Fetches OHLCV (Open, High, Low, Close, Volume) data from Binance for a given currency.

        Args:
            request_date (datetime): The date from which to start fetching the data.

        Returns:
            pd.DataFrame: A DataFrame containing OHLCV data.
        """
        # Initialize the Binance exchange using ccxt
        exchange = ccxt.binance()
        timeframe = '1m'  # 1-minute interval

        print(f"Requesting OHLCV data - Exchange: {exchange.name}, Currency: {self.currencies_[self.currency_]}, Timeframe: {timeframe}")
        result = []
        since = int(request_date.timestamp()) * 1000  # Convert to milliseconds

        # Fetch data in batches until no more data is available
        while True:
            print(f'Collecting data from: {datetime.fromtimestamp(since / 1000)}')
            try:
                ohlcv_response = exchange.fetch_ohlcv(self.currencies_[self.currency_], timeframe, since, limit=1000)
                if len(ohlcv_response) == 0:
                    break
                result.extend(ohlcv_response)
                since = ohlcv_response[-1][0] + 60000  # Add a minute to the last collected timestamp
            except Exception as e:
                print(f"Error occurred while fetching OHLCV data from {exchange.name}: {e}")
                raise

        # Create a DataFrame from the fetched OHLCV data
        df = pd.DataFrame(result, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')  # Convert to readable datetime
        df['trades'] = None  # Placeholder for the 'trades' column

        return df

    def insert_data_to_db(self, df: pd.DataFrame, chunk_size: int = 1000):
        """
        Inserts the fetched OHLCV data into a MySQL table in chunks.

        Args:
            df (pd.DataFrame): The DataFrame containing the OHLCV data.
            chunk_size (int): The size of the chunks for batch insertion (default is 1000).
        """
        # Establish MySQL connection
        connection = mysql.connector.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name
        )

        # Create a cursor object for executing queries
        cursor = connection.cursor()

        try:
            if not df.empty:
                print("Saving data to SQL table...")
                # Convert the datetime column to string format for MySQL
                df['datetime'] = df['datetime'].astype(str)

                # SQL query to insert data with placeholders
                insert_query = f"""
                INSERT IGNORE INTO {self.currency_} (datetime, open, high, low, close, volume, trades)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                # Insert data in chunks
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size]
                    data = chunk.values.tolist()
                    cursor.executemany(insert_query, data)

                # Commit the transaction
                connection.commit()

                print("Data Updated!")

        except Exception as e:
            connection.rollback()
            print(f"Failed to insert data into {self.currency_}: {e}")
            raise

        finally:
            cursor.close()
            connection.close()

    def refresh_data_every_minute(self):
        """
        Continuously fetches new OHLCV data every minute and updates the MySQL database.
        """
        while True:
            try:
                # Get the last recorded date from the database
                request_date = self.get_last_date()

                # Fetch new data from Binance starting from the last recorded date
                df = self.get_data(request_date)

                # Insert the fetched data into the database
                self.insert_data_to_db(df)

                # Sleep for 1 minute before the next update
                print("Waiting for 1 minute before the next update...")
                sleep(60)

            except Exception as e:
                print(f"Error during data refresh: {e}")
                sleep(60)  # If an error occurs, wait for 1 minute before retrying


def main():
    """
    The main function to update cryptocurrency data for multiple currencies.
    """
    # Create an instance of the UpdateCrypto class for BTCUSD
    crypto_updater = UpdateCrypto('BTCUSD')

    # Refresh the data every minute
    crypto_updater.refresh_data_every_minute()


# Call the main function
if __name__ == "__main__":
    main()

