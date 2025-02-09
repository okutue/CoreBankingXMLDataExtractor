# data_loader/main.py
import argparse
from .config import load_config, save_config
from .database import get_connection
from .extraction import get_view_definition, process_rows
from .processing import parse_view_mapping_xml, parse_view_mapping_nonxml
from .loader import create_target_table, load_data_to_target_multi
from .logging import logger

def main():
    parser = argparse.ArgumentParser(
        description="Extract and migrate data for multiple tables based on config file."
    )
    parser.add_argument("--config", default="config/config.json", help="Path to configuration JSON file")
    args = parser.parse_args()

    config = load_config()
    source_conf = config["source"]
    target_conf = config["target"]
    default_conf = config["default"]
    table_configs = config["tables"]

    batch_size = int(default_conf.get("batch_size", 1000))
    threads = int(default_conf.get("threads", 4))
    oldconfig = config

    # Build source and target connection parameters (as tuples)
    src_conn_params = (source_conf["server"], source_conf["database"], source_conf["username"], source_conf["password"])
    tgt_conn_params = (target_conf["server"], target_conf["database"], target_conf["username"], target_conf["password"])

    for tbl in table_configs:
        logger.info(f"Processing source table '{tbl['table']}' with view '{tbl['view']}' to target table '{tbl['target_table']}'")
        nonxml = tbl.get("nonxml", False)
        enabled = tbl.get("enabled",True)
        incremental_col = tbl.get("incremental_column", "").strip()
        last_value = tbl.get("incremental_value", "").strip()

        # Check if table is enabled for ETL
        if not enabled:
            logger.info(f"Skipping table '{tbl['table']}' as extraction is disabled in config")
            continue
        # Connect to source database and retrieve view definition and data
        src_conn = get_connection(*src_conn_params)
        mapping_cursor = src_conn.cursor()
        view_def = get_view_definition(mapping_cursor, tbl["view"])
        if view_def is None:
            logger.error(f"View definition for {tbl['view']} not found. Skipping table {tbl['table']}.")
            src_conn.close()
            continue

        # Get mapping based on transformation type
        if nonxml:
            mapping = parse_view_mapping_nonxml(view_def)
        else:
            mapping = parse_view_mapping_xml(view_def)

        if not mapping:
            logger.error("No mapping found. Skipping table.")
            src_conn.close()
            continue

        # logger.info("Mapping for output columns:")
        # for m in mapping:
            # logger.info(f"  {m}")
            
        src_cursor = src_conn.cursor()
            
        # Fully qualified source table name
        source_table_full = f"[{source_conf['schema']}].[{tbl['table']}]"
        
        # logger.info(f"Selecting from source table: {source_table_full}")
        query = f"SELECT RECID, XMLRECORD FROM {source_table_full} WITH (NOLOCK) OPTION (MAXDOP 1)"
        src_cursor.execute(query)        
        # src_cursor.execute(f"SELECT RECID, XMLRECORD FROM {source_table_full}")
        logger.info(f"Query executing for source table {source_table_full} using: {query}")
        # Process rows from source
        # logger.info(f"Processing rows from: {source_table_full}")
        processed_rows = process_rows(src_cursor, batch_size, threads, nonxml)
        src_conn.close()

        
        # If the incremental column is specified in the configuration, map its alias to the actual XML tag.
        incremental_col = None
        if "incremental_column" in tbl and tbl["incremental_column"].strip():
            incremental_alias = tbl["incremental_column"].strip()
            # For XML processing, mapping is assumed to be a list of tuples (xml_tag, alias)
            if not nonxml:
                for tag, alias in mapping:
                    if alias.lower() == incremental_alias.lower():
                        incremental_col = tag
                        break
                if incremental_col is None:
                    logger.error(f"Incremental column alias '{incremental_alias}' not found in view mapping for table '{tbl['table']}'.")
            else:
                # For non-XML processing, you might decide to use a specific field.
                # For this example, we assume RECID is used as the incremental field.
                incremental_col = "RECID"

        # Filter the processed rows based on incremental value
        filtered_rows = []
        new_max_value = last_value  # Will store the highest incremental value encountered

        for recid, record in processed_rows:
            current_val = None
            if incremental_col and not nonxml:
                current_val = record.get(incremental_col)
            elif incremental_col and nonxml:
                current_val = recid  # For non-XML, using RECID as the incremental value (adjust if needed)
            
            # If incremental filtering is enabled and a current value is found, decide whether to include the row.
            if incremental_col and current_val:
                # If last_value is provided, only include rows where current_val is greater.
                if last_value and current_val < last_value:
                    continue  # Skip this row as it is not new.
                # Update new_max_value if current_val is greater than the previous maximum.
                if new_max_value == "" or current_val >= new_max_value:
                    new_max_value = current_val
            # Include the row for insertion.
            filtered_rows.append((recid, record))

        logger.info(f"Total rows to insert after filtering: {len(filtered_rows)}")

        # Continue building the header and row values as before.
        if nonxml:
            header = ["RECID"] + [alias for (_, alias, _) in mapping]
        else:
            header = ["RECID"] + [alias for (_, alias) in mapping]
        header.append("TotalRecords")
        # logger.info(f"Final header: {header}")

        rows_to_insert = []
        for recid, record in filtered_rows:
            row = [recid]
            if nonxml:
                fields_taf, fields_ext = record
                for pos, _, func in mapping:
                    index = pos - 1
                    if func == "tafjfield":
                        value = fields_taf[index] if index < len(fields_taf) else ""
                    elif func == "extractValueJS":
                        value = fields_ext[index] if index < len(fields_ext) else ""
                    else:
                        value = ""
                    row.append(value)
            else:
                for xml_tag, _ in mapping:
                    row.append(record.get(xml_tag, ""))
            rows_to_insert.append(tuple(row))

        total_records = len(rows_to_insert)
        # Append the total_records value to each row as an extra column.
        rows_to_insert = [row + (total_records,) for row in rows_to_insert]
        logger.info(f"Total rows processed for table '{tbl['table']}': {total_records}")

        # If incremental filtering is in use, update the configuration with the new maximum value.
        if incremental_col:
            if new_max_value and new_max_value != last_value:
                tbl["incremental_value"] = new_max_value
                logger.info(f"Setting Incremental value for table '{tbl['table']}' to '{tbl['incremental_column']}': {new_max_value}")
            else:
                logger.info(f"No new incremental value found for table '{tbl['table']}'.")
        
        # Connect to target database, drop and create target table
        tgt_conn = get_connection(*tgt_conn_params)
        create_target_table(tgt_conn, target_conf["schema"], tbl["target_table"], header)
        tgt_conn.close()

        # Load data into target using multithreading bulk insert
        logger.info(f"Loading data into target table '{tbl['target_table']}' started.")
        load_data_to_target_multi(tgt_conn_params, target_conf["schema"], tbl["target_table"], header, rows_to_insert, threads, batch_size)
        logger.info(f"Data load complete for target table '{tbl['target_table']}'.")
        
        
    # Save the updated configuration back to file.
    save_config(config)
    if config != oldconfig:
        logger.info("Configuration updated with new incremental values for incremental extraction.")

if __name__ == '__main__':
    main()
