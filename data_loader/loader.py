# data_loader/loader.py
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from .database import get_connection
from .logging import logger

def create_target_table(target_conn, target_schema, target_table, header):
    """
    Drop the target table if it exists and then create it.
    All columns are NVARCHAR(MAX). Fully qualified name: [schema].[table]
    """
    cursor = target_conn.cursor()
    table_full_name = f"[{target_schema}].[{target_table}]"
    drop_query = f"DROP TABLE IF EXISTS {table_full_name};"
    logger.info(f"Dropping target table {table_full_name} if it exists...")
    cursor.execute(drop_query)
    columns_def = ",\n".join([f"[{col}] NVARCHAR(MAX)" for col in header])
    create_query = f"CREATE TABLE {table_full_name} (\n{columns_def}\n);"
    logger.info(f"Creating target table {table_full_name}.")
    cursor.execute(create_query)
    target_conn.commit()
    cursor.close()

def insert_chunk(chunk, tgt_conn_str, target_schema, target_table, header):
    """
    Insert a chunk of rows into the target table.
    Each call opens its own connection.
    """
    try:
        conn = get_connection(*tgt_conn_str)  # tgt_conn_str is a tuple: (server, database, username, password)
        cursor = conn.cursor()
        columns = ", ".join([f"[{col}]" for col in header])
        placeholders = ", ".join(["?" for _ in header])
        table_full_name = f"[{target_schema}].[{target_table}]"
        insert_query = f"INSERT INTO {table_full_name} ({columns}) VALUES ({placeholders})"
        #Use fast_executemany
        cursor.fast_executemany = True
        cursor.executemany(insert_query, chunk)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error inserting chunk: {e}")

def load_data_to_target_multi(tgt_conn_str, target_schema, target_table, header, rows, n_threads, chunk_size):
    """
    Insert the processed rows into the target table using multiple threads.
    Split rows into chunks of size chunk_size; each chunk is inserted concurrently.
    """
    total_rows = len(rows)
    n_chunks = ceil(total_rows / chunk_size)
    chunks = [rows[i * chunk_size:(i + 1) * chunk_size] for i in range(n_chunks)]
    logger.info(f"Inserting {total_rows} rows in {n_chunks} chunks using {n_threads} threads...")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(insert_chunk, chunk, tgt_conn_str, target_schema, target_table, header) for chunk in chunks]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in chunk insertion: {e}")
