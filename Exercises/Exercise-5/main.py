import psycopg2
import pandas as pd
import os
import csv
from pathlib import Path

def create_table(conn):
    cur = None
    try:
        cur = conn.cursor()
        commands = ("""
                    DROP TABLE IF EXISTS accounts CASCADE; 
                    CREATE TABLE accounts (
                        customer_id INTEGER PRIMARY KEY,
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        address_1 VARCHAR(255),
                        address_2 VARCHAR(255),
                        city VARCHAR(255),
                        state VARCHAR(255),
                        zip_code INTEGER,
                        join_date DATE
                    );
                    
                    DROP TABLE IF EXISTS products CASCADE;
                    CREATE TABLE products (
                        product_id INTEGER PRIMARY KEY, 
                        product_code VARCHAR(10), 
                        product_description TEXT
                    );
                    
                    DROP TABLE IF EXISTS transactions CASCADE;
                    CREATE TABLE transactions(
                        transaction_id VARCHAR(50) PRIMARY KEY, 
                        transaction_date DATE, 
                        product_id INTEGER, 
                        product_code VARCHAR(10), 
                        product_description TEXT, 
                        quantity INTEGER, 
                        account_id INTEGER,
                        CONSTRAINT fk_product
                            FOREIGN KEY (product_id)
                            REFERENCES products(product_id),
                        CONSTRAINT fk_account
                            FOREIGN KEY (account_id)
                            REFERENCES accounts(customer_id)
                    );

                    CREATE INDEX idx_transaction_date ON transactions(transaction_date);
                    CREATE INDEX idx_product_id ON transactions(product_id);
                    CREATE INDEX idx_account_id ON transactions(account_id);
                """)
    
    
        cur.execute(commands) 
        conn.commit()
        print("Tables created successfullt")
    
    except psycopg2.Error as e:
        print(f"Database error during table creation: {str(e)}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error during table creation: {str(e)}")
        conn.rollback()
        raise

    finally:
        if cur is not None:
            cur.close()


def csv_to_db(conn, csv_path):
    cur = None
    try:
        cur = conn.cursor()
        
        table_name = Path(csv_path).stem
        df = pd.read_csv(csv_path)

        for column in df.columns:
            if df[column].dtype == 'numpy.int64':
                df[column] = df[column].astype(int)
            elif df[column].dtype == 'numpy.float64':
                df[column] = df[column].astype(float)  # Convert to Python float
            else:
                df[column] = df[column].astype(str)
        
        # Create an INSERT statement with proper column names
        columns = ','.join(df.columns)
        values = ','.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        records = records = [tuple(x) for x in df.to_records(index=False)]
        cur.executemany(insert_query, records)
        conn.commit()
        print(f"Sunccessfully imported {csv_path}")
    
    except FileNotFoundError as e:
        print(f"CSV file not found: {csv_path}")
        conn.rollback()
        raise
    except psycopg2.Error as e:
        print(f"Database error: {csv_path}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        conn.rollback()
        raise

    finally:
        if cur is not None:
            cur.close()



def main():
    # 1. Examine each `csv` file in `data` folder. Design a `CREATE` statement for each file.
    # 2. Ensure you have indexes, primary and forgein keys.
    # 3. Use `psycopg2` to connect to `Postgres` on `localhost` and the default `port`.
    # 4. Create the tables against the database.
    # 5. Ingest the `csv` files into the tables you created, also using `psycopg2`.

    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = None
    
    try:
        # Connect to database
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

        # Create tables
        create_table(conn)

        # Get list of csv files
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        csv_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]

        orderd_files = []
        for table in ['accounts', 'products', 'transactions']:
            file = [f for f in csv_files if table in f][0]
            orderd_files.append(file)

        for csv_file in orderd_files:
            csv_to_db(conn, csv_file)
            print(f"Imported {csv_file}")
        
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error: {str(e)}")
        if conn is not None:
            conn.rollback()
            raise

    finally: 
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
