# data_loader/database.py
import pyodbc
from data_loader.logging import logger

def get_connection(server, database, username, password):
    """Establish a connection to SQL Server."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    try:
        connection = pyodbc.connect(conn_str)
        # logger.info(f"Successfully connected to {database} database on {server} server.")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to SQL Server '{server}' on database '{database}': {e}. Verify the connection sitring is configured properly in ./config/config.json file. If first execution config folder would be auto created")
        raise  # Optionally, you can choose to return None instead of raising the exception.
