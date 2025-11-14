import pyodbc
import sqlite3
import logging
from pathlib import Path

"""
    The script is used to convert Microsoft Access DB to SQL DB
    The script automates the process of:
        1. Connecting to an Access database.
        2. Fetching table information (schemas and data) from the Access database.
        3. Transferring the data and recreating the same tables in an SQLite database.
        4. Inserting the fetched data into the respective tables in SQLite.
"""

def create_sqlite_table_from_schema(sqlite_cursor, table_name, columns):
    """Create a SQLite table based on schema from the MDB database."""
    # Escape table and column names with double quotes
    escaped_table_name = f'"{table_name}"'
    column_definitions = ", ".join([f'"{col[0]}" {col[1]}' for col in columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {escaped_table_name} ({column_definitions});"
    sqlite_cursor.execute(create_table_query)



def get_table_schema(access_cursor, table_name):
    """Fetch table schema from the MDB database."""
    try:
        access_cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
        columns = [(desc[0], "TEXT") for desc in access_cursor.description]  # Default to TEXT type
        return columns
    except pyodbc.Error as e:
        logging.error(f"Error fetching schema for table {table_name}: {e}")
        return []


def fetch_data_from_table(access_cursor, table_name):
    """Fetch all rows from a table in the MDB database."""
    try:
        access_cursor.execute(f"SELECT * FROM {table_name}")
        return access_cursor.fetchall()
    except pyodbc.Error as e:
        logging.error(f"Error fetching data from table {table_name}: {e}")
        return []


def insert_data_to_sqlite(sqlite_cursor, table_name, columns, data):
    """Insert data into a SQLite table."""
    placeholders = ", ".join(["?"] * len(columns))
    column_names = ", ".join([col[0] for col in columns])
    insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    try:
        sqlite_cursor.executemany(insert_query, data)
    except sqlite3.Error as e:
        logging.error(f"Error inserting data into {table_name}: {e}")


def transfer_data(access_conn, sqlite_conn):
    access_cursor = access_conn.cursor()
    sqlite_cursor = sqlite_conn.cursor()

    # Add XmlStorage table to SQLite
    sqlite_cursor.execute("""
        CREATE TABLE IF NOT EXISTS XmlStorage (
            uid TEXT PRIMARY KEY,
            xml_content TEXT,
            status NUMERIC
        );
    """)

    # Get table names from the Access database
    access_cursor.tables(tableType='TABLE')
    tables = [row.table_name for row in access_cursor.fetchall()]

    for table_name in tables:
        logging.info(f"Processing table: {table_name}")
        
        # Fetch table schema
        columns = get_table_schema(access_cursor, table_name)
        if not columns:
            logging.warning(f"Skipping table {table_name} due to missing schema.")
            continue

        # Create the table in SQLite
        create_sqlite_table_from_schema(sqlite_cursor, table_name, columns)

        # Fetch data from Access
        data = fetch_data_from_table(access_cursor, table_name)
        if not data:
            logging.warning(f"No data found for table {table_name}.")
            continue

        # Insert data into SQLite
        insert_data_to_sqlite(sqlite_cursor, table_name, columns, data)

    sqlite_conn.commit()
    logging.info("Data transfer completed.")


def main():
    dbs = ["RTDPointCatalog_2025.0", "RTDPointCatalog_2023.1", "RTDPointCatalog_2023.0", "RTDPointCatalog_2022.1"]
    for db in dbs:
        access_db_path = str(Path(__file__).resolve().parent / f"{db}.mdb")
        sqlite_db_path = str(Path(__file__).resolve().parent / f"{db}.db")

        # Connect to Access database
        access_conn_str = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={access_db_path};"
            "PWD=shruti;" 
        )
        try:
            access_conn = pyodbc.connect(access_conn_str)
            logging.info("Connected to Access database.")
        except pyodbc.Error as e:
            logging.error(f"Failed to connect to Access database: {e}")
            return

        # Connect to SQLite database
        try:
            sqlite_conn = sqlite3.connect(sqlite_db_path)
            logging.info("Connected to SQLite database.")
        except sqlite3.Error as e:
            logging.error(f"Failed to connect to SQLite database: {e}")
            access_conn.close()
            return

        # Transfer data
        transfer_data(access_conn, sqlite_conn)

        # Close connections
        access_conn.close()
        sqlite_conn.close()
        logging.info("All tasks completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
