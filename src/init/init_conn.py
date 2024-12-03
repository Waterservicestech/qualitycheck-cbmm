from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import eel
from sqlalchemy import text

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
    Tests the connection to a server and returns a success message or error.

    Args:
        server_name (str): Name of the server to connect to.
        database_name (str): Name of the database.

    Returns:
        dict: A dictionary with 'success' (bool) and 'message' (str).
    """
    try:
        # Connect to the database
        conn = connect_to_database(server_name, database_name)
        
        if conn is None:
            message = "Connection failed: No valid connection object returned."
            print(message)
            return {"success": False, "message": message}

        # Define the test query
        test_query = text('SELECT TOP 10 [ID] FROM station')
        
        # Execute the query
        with conn.connect() as connection:
            connection.execute(test_query)
            message = "CONEXÃO BEM SUCEDIDA"
            print(message)
            return {"success": True, "message": message}

    except Exception as e:
        error_message = f"CONEXÃO FALHOU"
        print(e)
        return {"success": False, "message": error_message}