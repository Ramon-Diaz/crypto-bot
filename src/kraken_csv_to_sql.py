import os
import pandas as pd
import pathlib
from tqdm import tqdm
import mysql.connector

OHLCVT_DIRECTORY = pathlib.Path.cwd().joinpath('data')

# Establish the connection
connection = mysql.connector.connect(
    host='mysql',
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE', 'crypto_data')
)

# Create a cursor object
cursor = connection.cursor()

# Create a table to keep track of processed files
cursor.execute("""
CREATE TABLE IF NOT EXISTS processed_files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    processed_datetime DATETIME DEFAULT CURRENT_TIMESTAMP
);
""")

# Function to check if a file has already been processed
def is_file_processed(filename):
    cursor.execute("SELECT 1 FROM processed_files WHERE filename = %s", (filename,))
    return cursor.fetchone() is not None

# Function to mark a file as processed
def mark_file_as_processed(filename):
    try:
        cursor.execute("INSERT INTO processed_files (filename) VALUES (%s)", (filename,))
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Failed to mark file as processed: {e}")
        raise

# Function to insert data into MySQL table using batch inserts with chunking
def insert_data_to_db(df, table_name, chunk_size=1000):
    try:
        if not df.empty:
            # Convert datetime column to string format
            df['datetime'] = df['datetime'].astype(str)
            
            # Define the SQL query with placeholders
            insert_query = f"""
            INSERT INTO {table_name} (datetime, open, high, low, close, volume, trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                data = chunk.values.tolist()
                
                cursor.executemany(insert_query, data)
            connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Failed to insert data into {table_name}: {e}")
        raise

def import_csv(filePath):
    # Load the CSV file, automatically detecting the header row if it exists
    df = pd.read_csv(filePath, header=0 if 'timestamp' in open(filePath).readline() else None)
    
    # Define the expected columns and their types
    df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'trades']
    
    # Remove any rows where 'datetime' is not numeric
    df = df[pd.to_numeric(df['datetime'], errors='coerce').notnull()]
    
    # Convert the datetime column from Unix timestamp to datetime format
    df['datetime'] = pd.to_datetime(df['datetime'].astype(float), unit='s')
    
    # Convert datetime to string format suitable for SQL insertion
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Ensure the other columns are numeric
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'trades']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop any rows where critical numeric columns couldn't be converted
    df = df.dropna(subset=numeric_columns)
    
    return df

# Function to create a table for each currency
def create_table_for_currency(cursor, table_name):
    table_creation_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT,
            datetime DATETIME NOT NULL UNIQUE,
            open DECIMAL(18, 8) NOT NULL,
            high DECIMAL(18, 8) NOT NULL,
            low DECIMAL(18, 8) NOT NULL,
            close DECIMAL(18, 8) NOT NULL,
            volume DECIMAL(30, 8) NOT NULL,
            trades INT NOT NULL,
            PRIMARY KEY (id, datetime)
        );
    """
    cursor.execute(table_creation_query)

# Read and insert data file by file
interval = "1" # minute interval of data
csv_files = [f for f in os.listdir(OHLCVT_DIRECTORY) if f.endswith(f'_{interval}.csv')]

print(f"Processing files in directory: OHLCVT_DIRECTORY, with interval: {interval}")
for idx, file in tqdm(enumerate(csv_files), total=len(csv_files)):
    if is_file_processed(file):
        continue

    try:
        currency = file.split('_')[0]
        table_name = currency.replace(".", "_")
        
        # Create the table for the currency if it doesn't exist
        create_table_for_currency(cursor, table_name)

        filePath = OHLCVT_DIRECTORY.joinpath(file)
        if os.path.getsize(filePath) == 0:
            print(f"File {file} is empty. Skipping.")
            continue

        df = import_csv(filePath)
        insert_data_to_db(df, table_name)
        mark_file_as_processed(file)

    except Exception as e:
        print(f"Error processing file {file}: {e}")
        break  # Stop the script if an error occurs while processing a file

# Close the cursor and connection
cursor.close()
connection.close()

# temporal adjustment for mysql variables -- login as root to work
# set global max_allowed_packet=1000000000;
