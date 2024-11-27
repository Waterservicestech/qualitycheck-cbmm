from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import eel

def check_any_odbc_driver():
    """
    Checks for any ODBC drivers installed on the system.

    Returns:
        str: The name of the ODBC driver if found, otherwise None.
    """
    drivers = [driver for driver in pyodbc.drivers()]
    for driver in drivers:
        if "ODBC Driver" in driver and "SQL Server" in driver:
            return driver  # Returns the name of the found driver
    return None

def connect_to_database(server_name, database_name):
    """
    Establishes a connection to the database using a detected ODBC driver.

    Args:
        server_name (str): The name of the database server to connect to.
        database_name (str): The name of the target database.

    Returns:
        sqlalchemy.engine.Engine: A SQLAlchemy engine for the database connection.

    Raises:
        Exception: If no suitable ODBC driver is found.
    """
    odbc_driver = check_any_odbc_driver()

    if not odbc_driver:
        raise Exception("Error: No suitable ODBC driver found")

    connection_str = f'Driver={{{odbc_driver}}};Server={server_name};Database={database_name};Trusted_Connection=yes;Encrypt=no;'
    connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_str})
    conn = create_engine(connection_url)
    return conn

@eel.expose
def valid_connection(server_name, database_name):
    """
    Tests the connection to a server.
    Returns True if the test query can be executed.

    Args:
        server_name (str): Name of the server to connect to.
        database_name (str): Name of the database.

    Returns:
        bool: True if the connection and query were successful, False otherwise.
    """
    try:
        conn = connect_to_database(server_name, database_name)
        
        if conn is None:
            print("Connection failed: No valid connection object returned.")
            return False

        test_query = 'SELECT TOP 10 [ID] FROM station'

        # Test the query execution
        with conn.connect() as connection:
            connection.execute(test_query)
            return True

    except Exception as e:
        print(f"Error testing connection: {e}")
        return False

