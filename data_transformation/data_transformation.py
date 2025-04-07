# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 19:50:19 2025

@author: marcu
"""

import sqlite3
import re


from .constants import valid_states

class DatabaseConnector:
    def __init__(self, file_path=None):
        """
        Initialize the ExcelFormatter by leveraging PulledFilesExcelHandler
        to manage the Excel file interactions.
        """
        # Initialize database
        self.conn = None
        self.cursor = None
        self.initialize_database()


    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context, handling any necessary cleanup.
        """
        # Add any cleanup logic here if needed
        print("Exiting DatabaseConnector context.")

    def initialize_database(self):
        # Connect to (or create) a SQLite database file
        self.conn = sqlite3.connect("contact_info.db")
        self.cursor = self.conn.cursor()

    def check_and_drop_table(self):
        """Check if the Users table exists and drop it if it does."""
        self.cursor.execute("DROP TABLE IF EXISTS Donors")
        self.conn.commit()  # Commit the drop operation

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE Donors (
                Donor_ID INT PRIMARY KEY,
                Last_Name TEXT NOT NULL,
                First_Name TEXT NOT NULL,
                Address TEXT NOT NULL,
                City TEXT NOT NULL,
                State TEXT NOT NULL,
                Zip TEXT NOT NULL,
                Phone TEXT NOT NULL,
                Email NOT NULL
            )
        """)
        self.conn.commit()  # Commit changes to make sure the table is created

    def close_connection(self):
        self.conn.close()  # Close the database connection properly

    def insert_record(self, csv_Donor_ID, csv_Last_Name, csv_First_Name, csv_Address, csv_City, csv_State, csv_Zip, csv_Phone, csv_Email):
        """
           Insert a new donor record into the Donors table.

           This method executes an SQL INSERT statement to add a new record with the provided donor details.
           It commits the transaction to save changes to the database. If the insertion fails due to a
           UNIQUE constraint (for example, when a duplicate Donor_ID is encountered), the method catches
           the sqlite3.IntegrityError and prints an error message.

           Args:
               csv_Donor_ID (str or int): Unique identifier for the donor.
               csv_Last_Name (str): Donor's last name.
               csv_First_Name (str): Donor's first name.
               csv_Address (str): Donor's address.
               csv_City (str): Donor's city.
               csv_State (str): Donor's state.
               csv_Zip (str): Donor's ZIP code.
               csv_Phone (str): Donor's phone number.
               csv_Email (str): Donor's email address.

           Returns:
               None
           """
        try:
            self.cursor.execute("""
            INSERT INTO Donors (Donor_ID, Last_Name, First_Name, Address, City, State, Zip, Phone, Email) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (csv_Donor_ID, csv_Last_Name, csv_First_Name, csv_Address, csv_City, csv_State, csv_Zip, csv_Phone, csv_Email))

            self.conn.commit()

        except sqlite3.IntegrityError as e:
            print(f"❌ ERROR: UNIQUE constraint failed! Duplicate Donor_ID: {csv_Donor_ID}")

    # Quick check if the database has this Donor.
    def query_for_match_by_name(self, csv_last_name, csv_first_name):
        """
        Query the Donors table for a unique match by last name and first name.

        This method executes a SQL SELECT query to count the number of donor records
        that match the provided last name and first name. If exactly one matching record
        is found, the method returns True, indicating a unique match. Otherwise, it returns False.

        Args:
            csv_last_name (str): The last name to search for in the Donors table.
            csv_first_name (str): The first name to search for in the Donors table.

        Returns:
            bool: True if exactly one matching donor record is found, False otherwise.
        """
        self.cursor.execute("SELECT COUNT(*) FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()
        count_value = results[0][0]  # "results" is a list of rows, and each row is a tuple
        if count_value == 1:
            return True
        else:
            return False

    def query_for_address(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Address FROM Donors WHERE Last_Name = ? AND First_Name = ?",
                            (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_city(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT City FROM Donors WHERE Last_Name = ? AND First_Name = ?",
                            (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_state(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT State FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_zipcode(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Zip FROM Donors WHERE Last_Name = ? AND First_Name = ?",
                            (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_phone(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Phone FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_email(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Email FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]









class AddressParser:
    def __init__(self, file_path=None):
        self.file_path = file_path

    @staticmethod
    def separate_number_from_street(token):
        return re.sub(r'^(\d+)([A-Za-z].*)$', r'\1 \2', token)

    @staticmethod
    def seperate_zipcode_from_city(token):
        return re.sub(r'^([A-Za-z]+)(\d+)$', r'\1 \2', token)

    def transform_address(self, raw_line):
        """
        Convert a raw address line into "Street|City|State|Zip".

        If the line is empty -> return "EMPTY".
        If we cannot parse a ZIP (5 digits at end) -> "INCORRECT DATA".

        Steps:
          1. Combine 'College Station' into a single token 'CollegeStation'.
          2. Identify and strip off the 5-digit ZIP from the end.
          3. Split the remainder into tokens (on spaces or '|').
          4. The last token might be a recognized state or the city.
             If recognized state: state = 'TX', else default = 'TX'.
          5. Convert 'CollegeStation' back to 'College Station' if found in city.
          6. Replace '#' with 'Apartment'.
          7. Rebuild final as "Street|City|State|Zip".
        """
        line = raw_line.strip()
        if not line:
            return "EMPTY"

        # 1. Merge 'College Station' into a single token "CollegeStation"
        #    This way it won't get split into separate tokens.
        line = re.sub(r'(?i)\bcollege\s+station\b', 'CollegeStation', line)

        # 2. Look for a 5-digit ZIP at the end
        match_zip = re.search(r'\b(\d{5})\b\s*$', line)
        if not match_zip:
            return "INCORRECT DATA"
        zip_code = match_zip.group(1)
        remainder = line[:match_zip.start()].strip(",|. ")

        # 3. Split on spaces or '|'
        tokens = re.split(r'[|\s]+', remainder)
        tokens = [t for t in tokens if t.strip()]
        # Address = tokens[0]
        # City

        if not tokens:
            return "INCORRECT DATA"

        # 4. Check if the last token is a recognized state.
        #    We only handle 'TX' or 'Texas' => 'TX'.
        #    If so, the city is the second-last token. Otherwise city = last token, state = 'TX'.
        last_tok_lower = tokens[-1].lower()
        if last_tok_lower in valid_states:
            state = valid_states[last_tok_lower]
            if len(tokens) < 2:
                return "INCORRECT DATA"
            raw_city = tokens[-2]
            street_tokens = tokens[:-2]
        else:
            state = "TX"
            raw_city = tokens[-1]
            street_tokens = tokens[:-1]

        # 5. Convert 'CollegeStation' → 'College Station' in the city name
        raw_city = re.sub(r'(?i)collegestation', 'College Station', raw_city)

        # Also handle "CS" => "College Station" if needed
        if raw_city.lower() == "cs":
            raw_city = "College Station"

        # Separate merged City name from Zipcode
        raw_city = AddressParser.seperate_zipcode_from_city(raw_city)

        city = raw_city.strip(".,").title()

        # 6. Clean up the street tokens
        cleaned_street_parts = []
        for t in street_tokens:
            # Replace '#' with 'Apartment'
            t = t.replace('#', 'Apartment ')
            t = re.sub(r'(?i)Street', 'St', t)
            t = re.sub(r'(?i)Dr\.', 'Dr', t)
            t = re.sub(r'(?i)Circle', 'Cir', t)
            # Separate merged house numbers from street
            t = self.separate_number_from_street(t)
            # Strip trailing punctuation
            t = t.strip(".,").title()
            cleaned_street_parts.append(t)

        street = " ".join(cleaned_street_parts).strip()
        if not street:
            return "INCORRECT DATA"

        return f"{street}|{city}|{state}|{zip_code}"
