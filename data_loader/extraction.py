# data_loader/extraction.py
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from .processing import parse_extracted_xml_record, parse_delimited_record
from .database import get_connection
from .logging import logger

def get_view_definition(cursor, view_name):
    """
    Retrieve the view definition from sys.sql_modules using the view name.
    """
    query = """
    SELECT m.definition
    FROM sys.sql_modules m
    JOIN sys.views v ON m.object_id = v.object_id
    WHERE v.name = ?
    """
    cursor.execute(query, (view_name,))
    row = cursor.fetchone()
    return row.definition if row else None

def process_rows(src_cursor, batch_size, thread_count, nonxml):
    """
    Process rows from the source table in batches using multiple threads.
    Depending on the flag, use XML processing or non-XML processing.
    Returns a list of tuples (RECID, record).
    """
    # erecords = src_cursor.fetchall()
    # # Count the total number of records
    # total_erecords = len(erecords)
    # logger.info(f"Total number of extracted records to process: {total_erecords}")
    results = []
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        while True:
            batch = src_cursor.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                recid = row.RECID
                xmlrecord = row.XMLRECORD
                if nonxml:
                    # For non-XML, we use the RECID field for tafjfield splitting and XMLRECORD for extractValueJS splitting.
                    futures.append(executor.submit(parse_delimited_record, recid, recid, xmlrecord))
                else:
                    futures.append(executor.submit(parse_extracted_xml_record, recid, xmlrecord))
        for future in as_completed(futures):
            results.append(future.result())
    return results
