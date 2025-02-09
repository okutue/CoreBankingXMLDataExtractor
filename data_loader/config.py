# data_loader/config.py
import json
import os
import sys

def get_base_dir():
    """Return the directory of the executable or current file."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_dir()
CONFIG_DIR = os.path.join(BASE_DIR, "config")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

DEFAULT_CONFIG = {
    "source": {
        "server": "YOUR_SOURCE_SERVER",
        "database": "SourceDB",
        "username": "SourceUser",
        "password": "SourcePassword",
        "schema": "dbo"
    },
    "target": {
        "server": "YOUR_TARGET_SERVER",
        "database": "TargetDB",
        "username": "TargetUser",
        "password": "TargetPassword",
        "schema": "dbo"
    },
    "default": {
        "batch_size": 1000,
        "threads": 4,
        "log_max_size": 1048576,  # 1 MB in bytes
        "log_backup_count": 5
    },
    "tables": [
        {
            "table": "SourceTable1",
            "view": "View1",
            "target_table": "TargetTable1",
            "nonxml": False,
            "incremental_column":"",
            "incremental_value":"",
            "enabled": True
        },
        {
            "table": "SourceTable2",
            "view": "View2",
            "target_table": "TargetTable2",
            "nonxml": True,
            "incremental_column":"",
            "incremental_value":"",
            "enabled": True
        }
    ]
}
 
def ensure_config():
    """Ensure that the configuration directory and file exist; if not, create them with default config."""
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4)
    return CONFIG_FILE

def load_config(config_path=None):
    """Load configuration from the given JSON file path (auto-create if necessary)."""
    config_path = config_path or ensure_config()
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config

def save_config(config_data, config_path=None):
    config_path = config_path or ensure_config()
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4)
    except Exception as e:
        print(f"Error saving config: {e}")