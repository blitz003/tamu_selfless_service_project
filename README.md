# Data Transformation Project

This **data_transformation** project provides a set of Python scripts and modules for cleaning CSV data, parsing addresses, managing a SQLite database of donor contact information, and generating a transformed CSV file. The repository also includes a secondary script that de-duplicates donor contact information by last name and first name.

## Overview

1. **Data Cleaning & Transformation**  
   - Automatically parse and clean address fields (`AddressParser` class).
   - Normalize organization names vs. personal names.
   - Tokenize and re-map data from one set of headers to a new set.
   - Validate that CSV files contain the required headers.

2. **CSV Handling**  
   - Custom `CSVHandler` and `CSVWriter` classes for file read/write operations.
   - Validation of required headers using `ensure_headers_exist`.

3. **SQLite Database Management**  
   - `DatabaseConnector` class to create and initialize the SQLite database.
   - Methods to insert records, query for duplicates, and retrieve donor contact info.
   - Stores donor data (address, phone, email, etc.) and allows queries to fill missing info.

4. **Secondary Script (`keep_unique_rows`)**  
   - A simple standalone Python script to remove duplicate records from a CSV file.  
   - De-duplicates by last and first name, ensuring each name pair appears only once.

## Project Structure


### Key Files

- **`constants.py`**  
  Contains lists and dictionaries (`required_headers`, `contact_list_headers`, `output_headers`, `valid_states`) used throughout the project for validation and mapping.

- **`csv_handler.py`**  
  - `CSVHandler`: Reads CSV files, checks for headers, and retains records in a list.  
  - `CSVWriter`: Writes processed records to CSV files, optionally adding suffixes to filenames.  
  - `MissingHeaderException`: Custom exception when required headers are missing.

- **`data_transformation.py`**  
  - `AddressParser`: Cleans and standardizes address data into a “Street|City|State|Zip” format.  
  - `DatabaseConnector`: Connects to a SQLite database, creates tables, inserts records, and queries for donor information (address, city, state, ZIP, phone, email).

- **`keep_unique_rows.py`** (Secondary Script)  
  - Standalone utility to remove duplicate rows from a CSV based on `LastN` + `FirstN`.  
  - Useful for de-duplicating donor records before loading them into the main script.

## Requirements

- Python 3.7+  
- Libraries:
  - `csv` (standard library)
  - `re` (standard library)
  - `sqlite3` (standard library)
  - `datetime` (standard library)
  - `sys` (standard library)

## Setup & Installation

1. **Clone or Download** this repository.  



