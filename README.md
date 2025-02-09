# Data Loader

This package extracts data from SQL Server source tables (using a view for column mapping) and migrates it to a target SQL Server database. It supports both XML and non-XML formats and can be configured to process multiple tables via a JSON config file.

## Installation

1. Install dependencies:
pip install -r requirements.txt


2. Install the package:
pip install -e .


## Usage

Edit the configuration in `config/config.json` to add your source/target details and table definitions.

Run the application:
data-loader --config config/config.json

