# data_loader/logging.py
import logging
import os
import sys
import time
import glob
from logging.handlers import RotatingFileHandler
import gzip
import shutil
from .config import load_config, get_base_dir

# Determine the base directory (where the exe or script resides)
BASE_DIR = get_base_dir()

# Ensure the logs directory exists
LOG_DIR = os.path.join(BASE_DIR, "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Load log_max_size from config; default to 1 MB if not set.
try:
    config = load_config()
    log_max_size = int(config.get("default", {}).get("log_max_size", 1048576))
    log_backup_count = int(config.get("default", {}).get("log_backup_count", 5))
except Exception:
    log_max_size = 1048576  # 1 MB
    log_backup_count = 5

class CompressedRotatingFileHandler(RotatingFileHandler):
    def getFilesToDelete(self):
        """
        Return a list of backup log files that should be deleted.
        This handler names backups as baseFilename.DATE.gz.
        """
        dirName, baseName = os.path.split(self.baseFilename)
        pattern = os.path.join(dirName, baseName + ".*.gz")
        files = glob.glob(pattern)
        files.sort(key=os.path.getmtime)  # Oldest first
        if len(files) <= self.backupCount:
            return []
        return files[:len(files) - self.backupCount]

    def doRollover(self):
        """
        Perform a rollover. Rename the current log file by appending the current date and time,
        compress the backup file, and then reopen the log file for new log entries.
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # Generate a timestamped filename.
        dt = time.strftime("%Y%m%d_%H%M%S")
        rollover_filename = f"{self.baseFilename}.{dt}"

        # Rotate: rename the current log file to the new timestamped filename.
        self.rotate(self.baseFilename, rollover_filename)

        # Compress the rotated file.
        with open(rollover_filename, 'rb') as f_in:
            with gzip.open(rollover_filename + ".gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(rollover_filename)

        # Delete older backups if necessary.
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                if os.path.exists(s):
                    os.remove(s)

        # Reopen the log file stream.
        self.mode = 'a'
        self.stream = self._open()

def setup_logging():
    """Set up logging with file rotation (with compression) and console output. Avoid duplicate handlers."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # Check if handlers already exist; if so, don't add new ones.
    if logger.hasHandlers():
        return logger

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = CompressedRotatingFileHandler(LOG_FILE, maxBytes=log_max_size, backupCount=log_backup_count)
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.DEBUG)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Logging system initialised.")
    return logger

logger = setup_logging()
