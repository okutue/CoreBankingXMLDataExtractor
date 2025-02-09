import json
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# Default configuration keys for each section.
SOURCE_KEYS = ["server", "database", "username", "password", "schema"]
TARGET_KEYS = ["server", "database", "username", "password", "schema"]
DEFAULT_KEYS = ["batch_size", "threads", "log_max_size","log_backup_count"]
TABLE_KEYS = ["table", "view", "target_table", "nonxml","incremental_column","incremental_value","enabled"]

# Default config file path (in a "config" subfolder)
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

def get_config_file():
    """Return the config file path; ensure the config directory exists."""
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)
    return CONFIG_FILE

def ensure_config():
    """Ensure that the configuration file exists; if not, create it with default values."""
    config_path = get_config_file()
    if not os.path.exists(config_path):
        with open(config_path, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4)
    return config_path

def load_config(config_path=None):
    config_path = config_path or ensure_config()
    with open(config_path, "r") as f:
        return json.load(f)

def save_config(config_data, config_path=None):
    config_path = config_path or get_config_file()
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=4)

# Global dictionaries to hold entry widgets for each section.
entry_vars = {
    "source": {},
    "target": {},
    "default": {}
}
# List to hold dictionaries for each table row.
table_vars = []

def update_ui(config_data):
    # Update Source, Target, and Default sections based on expected keys.
    for section, keys in zip(["source", "target", "default"], [SOURCE_KEYS, TARGET_KEYS, DEFAULT_KEYS]):
        for key in keys:
            if key in config_data.get(section, {}):
                entry_vars[section][key].delete(0, tk.END)
                entry_vars[section][key].insert(0, str(config_data[section][key]))
    # Clear and update Tables section.
    for widget in table_container.winfo_children():
        widget.destroy()
    table_vars.clear()
    for table in config_data.get("tables", []):
        add_table(table)

def browse_config():
    global CONFIG_FILE
    file_path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
    if file_path:
        CONFIG_FILE = file_path
        update_ui(load_config(CONFIG_FILE))

def save_config_ui():
    try:
        new_data = {
            "source": {key: entry_vars["source"][key].get() for key in SOURCE_KEYS},
            "target": {key: entry_vars["target"][key].get() for key in TARGET_KEYS},
            "default": {key: entry_vars["default"][key].get() for key in DEFAULT_KEYS},
            "tables": [{key: table_vars[i][key].get() for key in table_vars[i]} for i in range(len(table_vars))]
        }
        # Convert nonxml field to Boolean for each table.
        for table in new_data["tables"]:
            val = str(table.get("nonxml", "")).lower()
            table["nonxml"] = True if val in ["true", "1", "yes"] else False
        # Convert enabled field to Boolean for each table.
        for table in new_data["tables"]:
            val = str(table.get("enabled", "")).lower()
            table["enabled"] = True if val in ["true", "1", "yes"] else False
        save_config(new_data, CONFIG_FILE)
        messagebox.showinfo("Success", "Config file saved successfully!")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to save config: {e}")

# def add_table(existing_data=None):
    # i = len(table_vars)
    # table_vars.append({})
    
    # row_frame = tk.Frame(table_container, bd=2, relief="groove", padx=5, pady=5)
    # row_frame.pack(fill="x", pady=3, padx=5)
    
    # for key in TABLE_KEYS:
        # sub_frame = tk.Frame(row_frame)
        # sub_frame.pack(fill="x", pady=2)
        # tk.Label(sub_frame, text=key.replace("_"," ").capitalize()+":", width=20, anchor="w").pack(side="left", padx=5)
        # entry = tk.Entry(sub_frame, width=40)
        # entry.pack(side="left", padx=5)
        # if existing_data and key in existing_data:
            # entry.insert(0, str(existing_data[key]))
        # table_vars[i][key] = entry

    # remove_button = tk.Button(row_frame, text="❌", command=lambda: remove_table(row_frame, i))
    # remove_button.pack(side="right", padx=5)
def add_table(existing_data=None):
    i = len(table_vars)
    table_vars.append({})
    
    row_frame = tk.Frame(table_container, bd=2, relief="groove", padx=5, pady=5)
    row_frame.pack(fill="x", pady=3, padx=5)
    
    for key in TABLE_KEYS:
        sub_frame = tk.Frame(row_frame)
        sub_frame.pack(fill="x", pady=2)
        tk.Label(sub_frame, text=key.replace("_"," ").capitalize()+":", width=20, anchor="w").pack(side="left", padx=5)
        
        if key == "nonxml":
            # Create a dropdown (OptionMenu) for nonxml with options "True" and "False"
            var = tk.StringVar()
            # If existing_data is provided, use its value; otherwise, default to "False"
            if existing_data and key in existing_data:
                var.set("True" if str(existing_data[key]).lower() in ["true", "1", "yes"] else "False")
            else:
                var.set("False")
            option_menu = tk.OptionMenu(sub_frame, var, "True", "False")
            option_menu.pack(side="left", padx=5)
            table_vars[i][key] = var
        elif key == "enabled":
             # Create a dropdown (OptionMenu) for enabled with options "True" and "False"
            var = tk.StringVar()
            # If existing_data is provided, use its value; otherwise, default to "False"
            if existing_data and key in existing_data:
                var.set("True" if str(existing_data[key]).lower() in ["true", "1", "yes"] else "False")
            else:
                var.set("True")
            option_menu = tk.OptionMenu(sub_frame, var, "True", "False")
            option_menu.pack(side="left", padx=5)
            table_vars[i][key] = var       
        else:
            var = tk.StringVar(value=str(existing_data[key]) if existing_data and key in existing_data else "")
            entry = tk.Entry(sub_frame, textvariable=var, width=40)
            entry.pack(side="left", padx=5)
            table_vars[i][key] = var

    remove_button = tk.Button(row_frame, text="❌", command=lambda: remove_table(row_frame, i))
    remove_button.pack(side="right", padx=5)

def remove_table(frame, index):
    frame.destroy()
    try:
        table_vars.pop(index)
    except Exception as e:
        print(f"Error removing table: {e}")

# Build the main GUI window.
root = tk.Tk()
root.title("Config Editor")
root.geometry("700x600")

# Create a main frame that will be scrollable.
main_frame = tk.Frame(root)
main_frame.pack(fill="both", expand=True)

# Create a canvas in the main frame.
canvas = tk.Canvas(main_frame)
canvas.pack(side="left", fill="both", expand=True)

# Add a vertical scrollbar to the canvas.
v_scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
v_scrollbar.pack(side="right", fill="y")
canvas.configure(yscrollcommand=v_scrollbar.set)

# Create a frame inside the canvas that will hold all configuration sections.
scrollable_frame = tk.Frame(canvas)
canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

def on_configure(event):
    canvas.configure(scrollregion=canvas.bbox("all"))

scrollable_frame.bind("<Configure>", on_configure)

# Add Source, Target, and Default sections to scrollable_frame.
sections = [("source", SOURCE_KEYS), ("target", TARGET_KEYS), ("default", DEFAULT_KEYS)]
for section, keys in sections:
    frame = tk.LabelFrame(scrollable_frame, text=section.capitalize(), padx=10, pady=5)
    frame.pack(fill="x", padx=10,pady=5)
    for i, key in enumerate(keys):
        tk.Label(frame, text=key.replace("_"," ").capitalize()+":", width=20, anchor="w").grid(row=i, column=0, padx=10, pady=2)
        entry = tk.Entry(frame, width=50)
        entry.grid(row=i, column=1, padx=10, pady=2)
        entry_vars[section][key] = entry

# Add Tables section to scrollable_frame.
tables_frame = tk.LabelFrame(scrollable_frame, text="Tables", padx=10, pady=5)
tables_frame.pack(fill="both", padx=10, pady=5)
# Create a dedicated container for table rows.
table_container = tk.Frame(tables_frame)
table_container.pack(fill="both", expand=True)

# Add Table button inside the tables_frame (this button will scroll with the content)
tk.Button(tables_frame, text="➕ Add Table", command=add_table).pack(pady=5)

# Fixed bottom frame for config file operations.
button_frame = tk.Frame(root)
button_frame.pack(pady=10)
tk.Button(button_frame, text="Browse Config", command=browse_config).grid(row=0, column=0, padx=5)
tk.Button(button_frame, text="Save Config", command=save_config_ui).grid(row=0, column=1, padx=5)

# Load configuration on startup.
CONFIG_FILE = get_config_file()
update_ui(load_config(CONFIG_FILE))

root.mainloop()
