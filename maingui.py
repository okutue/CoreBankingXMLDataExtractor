# data_loader/main_gui.py
import tkinter as tk
from tkinter import scrolledtext, ttk, messagebox
import subprocess
import threading
import sys
import os
import time
import logging

# For Windows, we can import this constant from subprocess (available on Python 3.7+)
import subprocess
if hasattr(subprocess, "CREATE_NO_WINDOW"):
    CREATE_NO_WINDOW = subprocess.CREATE_NO_WINDOW
else:
    # Otherwise, use the constant value (0x08000000)
    CREATE_NO_WINDOW = 0x08000000

# Function to get the directory where the executable/script is located.
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

# ------------------------------
# Logging Setup to a Tkinter Text Widget
# ------------------------------
class TextHandler(logging.Handler):
    """A logging handler that outputs to a Tkinter Text widget."""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + "\n")
            self.text_widget.configure(state='disabled')
            self.text_widget.yview(tk.END)
        self.text_widget.after(0, append)

def setup_logging(text_widget):
    logger = logging.getLogger("DataLoaderGUI")
    logger.setLevel(logging.DEBUG)
    # Clear any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create console handler (optional)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)

    # Create Text widget handler
    text_handler = TextHandler(text_widget)
    text_handler.setFormatter(formatter)
    text_handler.setLevel(logging.INFO)

    logger.addHandler(console_handler)
    logger.addHandler(text_handler)
    logger.info("Logging system initialised.")
    return logger

# ------------------------------
# Main GUI Application
# ------------------------------
class DataLoaderGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("XML Data Extractor Application")
        self.geometry("900x700")
        
        # Set the taskbar icon (ensure 'your_icon.ico' exists in the same folder)
        icon_path = os.path.join(get_base_dir(), "icondataloader.ico")
        if os.path.exists(icon_path):
            self.iconbitmap(icon_path)
        
        # Top frame with two buttons.
        top_frame = tk.Frame(self)
        top_frame.pack(pady=10)
        
        self.config_button = tk.Button(top_frame, text="Edit Config", command=self.launch_config_editor)
        self.config_button.pack(side="left", padx=10)
        
        self.dataloader_button = tk.Button(top_frame, text="Run XMLDataExtractor", command=self.launch_dataloader)
        self.dataloader_button.pack(side="left", padx=10)
        
        # Log display (scrolled text widget) with white background.
        self.log_text = scrolledtext.ScrolledText(self, state="disabled", bg="white", fg="black", width=100, height=30)
        self.log_text.pack(pady=10, padx=10)
        
        # Setup logging to the text widget.
        self.logger = setup_logging(self.log_text)
    
    def launch_config_editor(self):
        """Launch the configuration editor executable."""
        try:
            # Assuming config_editor.exe is in the same directory as this executable.
            config_editor_path = os.path.join(get_base_dir(), "config_editor.exe")
            subprocess.Popen([config_editor_path], creationflags=CREATE_NO_WINDOW)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to launch config editor: {e}")
    
    def launch_dataloader(self):
        """Launch the dataloader executable and capture its output."""
        try:
            self.dataloader_button.config(state="disabled")
            # Assuming dataloader.exe is in the same directory as this executable.
            dataloader_path = os.path.join(get_base_dir(), "XMLDataExtractor.exe")
            # Launch the process without showing a command prompt window.
            process = subprocess.Popen(
                [dataloader_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                creationflags=CREATE_NO_WINDOW
            )
            threading.Thread(target=self.read_process_output, args=(process,), daemon=True).start()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to launch XMLDataExtractor: {e}")
            self.dataloader_button.config(state="normal")
    
    def read_process_output(self, process):
        """Read output from the DataLoader process and append to log."""
        for line in iter(process.stdout.readline, ""):
            self.append_log(line)
        process.stdout.close()
        process.wait()
        self.append_log("XMLDataExtractor process completed.\n")
        self.dataloader_button.config(state="normal")
    
    def append_log(self, message):
        """Append a message to the log display in a thread-safe manner."""
        def update():
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, message)
            self.log_text.configure(state="disabled")
            self.log_text.yview(tk.END)
        self.log_text.after(0, update)

if __name__ == '__main__':
    app = DataLoaderGUI()
    app.mainloop()
