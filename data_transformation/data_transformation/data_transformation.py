# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 19:50:19 2025

@author: marcu
"""

import xlwings as xw
import pandas as pd
import sqlite3


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
        self.cursor.execute("SELECT COUNT(*) FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()
        count_value = results[0][0]  # "results" is a list of rows, and each row is a tuple
        if count_value == 1:
            return True
        else:
            return False

    def query_for_address(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Address FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_city(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT City FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_state(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT State FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
        results = self.cursor.fetchall()

        return results[0][0]

    def query_for_zipcode(self, csv_last_name, csv_first_name):
        self.cursor.execute("SELECT Zip FROM Donors WHERE Last_Name = ? AND First_Name = ?", (csv_last_name, csv_first_name))
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




