# data_loader/conversion.py

from datetime import datetime
from .logging import logger

def convert_value(value):
    """
    Attempt to convert the given value to an int, then a float, then to a datetime.
    If all conversions fail, return the value as a string.
    
    :param value: The value to convert (usually a string).
    :return: The converted value as int, float, datetime, or as a string.
    """
    if value is None:
        return None

    # Try to convert to integer.
    try:
        return int(value)
        # logger.info(f"This is int value: {value}")
    except (ValueError, TypeError):
        pass
    
    # Try to convert to float.
    try:
        return float(value)
        # logger.info(f"This is float value: {value}")
    except (ValueError, TypeError):
        pass

    # Try to convert to datetime using several common formats.
    date_formats = [
        "%Y-%m-%d %H:%M:%S",  # e.g., 2024-01-31 14:30:00
        "%Y-%m-%d",           # e.g., 2024-01-31
        "%m/%d/%Y %H:%M:%S",   # e.g., 01/31/2024 14:30:00
        "%m/%d/%Y",           # e.g., 01/31/2024
        "%d/%m/%Y %H:%M:%S",   # e.g., 31/01/2024 14:30:00
        "%d/%m/%Y",            # e.g., 31/01/2024
        "%H:%M:%S:%f %d %b %Y"    # e.g., 23:59:59:099 25 JUN 2020
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(value, fmt)
            # logger.info(f"This is date value: {value}")
        except (ValueError, TypeError):
            continue

    # If all conversion attempts fail, return the value as a string.
    return str(value)
