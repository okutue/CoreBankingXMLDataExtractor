# data_loader/processing.py
import re
import xml.etree.ElementTree as ET
from .config import load_config
from .logging import logger
# from .config import  # (if you want to import constants from config.py, e.g., delimiters)

# Constants for delimiters (could also be placed in config.py)
MULTI_VALUE_DELIMITER = ""
NONXML_TAF_DELIMITER = "*"
NONXML_EXT_DELIMITER = ""

def parse_view_mapping_xml(view_def):
    """
    Parse the view definition (SQL text) to extract the mapping of XML columns.
    Looks for expressions of the form:
      a.XMLRECORD.value('data(/row/cX)[1]', 'nvarchar(max)') "ALIAS"
    Returns a list of tuples (xml_tag, alias) in order.
    """
    pattern = r'a\.XMLRECORD\.value\(\s*\'data\(/row/(c\d+)\)\[1\]\'\s*,\s*\'nvarchar\(max\)\'\s*\)\s*"([^"]+)"'
    matches = re.findall(pattern, view_def, re.IGNORECASE)
    if not matches:
        logger.error("No XML extraction mapping found in the view definition.")
    return matches

def parse_view_mapping_nonxml(view_def):
    """
    Parse the view definition for non‐XML tables.
    Searches for:
      dbo.tafjfield(a.RECID, '*', 'N', '-2147483648') "ALIAS"
      dbo.extractValueJS(a.XMLRECORD, N, -?\d+) "ALIAS"
    Returns a list of tuples (position, alias, func).
    """
    pattern = r"""
    (?P<full>
      dbo\.tafjfield\(\s*a\.RECID\s*,\s*'(?P<taf_delim>\*)'\s*,\s*'(?P<taf_pos>\d+)'\s*,\s*'-2147483648'\s*\)\s*"(?P<taf_alias>[^"]+)"
    |
      dbo\.extractValueJS\(\s*a\.XMLRECORD\s*,\s*(?P<ext_pos>\d+)\s*,\s*-?\d+\s*\)\s*"(?P<ext_alias>[^"]+)"
    )
    """
    mapping = []
    for match in re.finditer(pattern, view_def, re.IGNORECASE | re.VERBOSE):
        if match.group('taf_alias'):
            pos = int(match.group('taf_pos'))
            alias = match.group('taf_alias')
            func = "tafjfield"
            mapping.append((pos, alias, func))
        elif match.group('ext_alias'):
            pos = int(match.group('ext_pos'))
            alias = match.group('ext_alias')
            func = "extractValueJS"
            mapping.append((pos, alias, func))
    if not mapping:
        logger.error("No non‐XML mapping found in the view definition.")
    return mapping

def parse_extracted_xml_record(recid, xml_record):
    """
    Parse an XML record (from the source table) into a dictionary.
    Keys are XML element tags; if an element appears more than once, concatenate with MULTI_VALUE_DELIMITER.
    """
    record_dict = {}
    try:
        root = ET.fromstring(xml_record)
        for child in root:
            tag = child.tag
            value = child.text if child.text is not None else ""
            if tag in record_dict:
                record_dict[tag] = record_dict[tag] + MULTI_VALUE_DELIMITER + value
            else:
                record_dict[tag] = value
    except ET.ParseError as e:
        logger.error(f"Error parsing XML for RECID {recid}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error for RECID {recid}: {e}")
    return recid, record_dict

def parse_delimited_record(recid, recid_str, xmlrecord_str):
    """
    For non‐XML tables: split the input values into two sets.
    Split RECID (using NONXML_TAF_DELIMITER) and XMLRECORD (using NONXML_EXT_DELIMITER).
    Returns (RECID, (fields_taf, fields_ext)).
    """
    fields_taf = recid_str.split(NONXML_TAF_DELIMITER)
    fields_ext = xmlrecord_str.split(NONXML_EXT_DELIMITER)
    return recid, (fields_taf, fields_ext)
