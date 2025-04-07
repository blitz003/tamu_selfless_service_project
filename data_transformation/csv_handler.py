import csv
import os
from datetime import datetime
from data_transformation.constants import base_csv_directory
from tkinter import Tk, filedialog

class MissingHeaderException(Exception):
    def __init__(self, missing_headers):
        self.missing_headers = missing_headers
        super().__init__(f"The provided CSV file is missing the required headers: \n{'  |  '.join(missing_headers)}\n")

class CSVHandler:
    def __init__(self, file_path=None):
        """
        Initialize the CSVHandler with an optional filepath
        If no filepath provided, prompt user to select one.
        """
        self.file_path = file_path
        self.data = []
        self.headers = []


        # Prompt for file if not provided
        if not self.file_path:
            self.file_path = self._prompt_for_file()

    def _prompt_for_file(self):
        # Prompt the user to select a CSV file using a file dialog.
        # This will prompt them to the base_csv_directory as defined in
        # finalizer.constants
        root = Tk()
        root.withdraw() # Hide the root Tkinter window
        initial_dir = base_csv_directory

        # Code block for creating contact_list sqlite db.
        if not os.path.exists(initial_dir):
            print(f"Initial directory does not exists: {initial_dir}. \n"
                  f"Falling back to: {os.getcwd()}")
            initial_dir = os.getcwd()  # Fall back to the current working directory.

        file_path = filedialog.askopenfilename(
            initialdir=initial_dir,
            title="Select A CSV File",
            filetypes=(("CSV Files", "*.csv"), ("All Files", "*.*"))
        )

        if file_path:
            print(f"Selected file: {file_path}")
            self.ala_file_name = os.path.basename(file_path)  # Save the file name to a variable
            self.ala_file_name_suffix = self.ala_file_name[:-4][-6:]  # Last 6 characters, excluding .csv (i.e. "ML ALA")
            return file_path
        else:
            raise FileNotFoundError("No CSV file was selected. The program will now close.")

    def ensure_headers_exist(self, required_headers):
        missing_headers = [header for header in required_headers if header not in self.headers]
        if missing_headers:
            raise MissingHeaderException(missing_headers)

    def read(self):
        # Load the CSV file into memory
        if not self.file_path:
            print("No file path provided.")
            return

        try:
            with open(self.file_path, mode="r", newline="", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                self.headers = reader.fieldnames  # Populate the headers attribute
                self.data = [row for row in reader]
                print(f"Loaded {len(self.data)} records from {self.file_path}")
        except FileNotFoundError:
            print("Error: The file was not found.")
        except Exception as e:
            print(f"An unexpected error occurred while reading CSV: {e}")

    def get_records(self):
        # Return all records from CSV file.
        return self.data

    def read_headers(self):
        """
        Reads only the headers of the CSV file without loading all the data.
        """
        if not self.file_path:
            print("No file path provided.")
            return None

        try:
            with open(self.file_path, mode="r", newline="", encoding="utf-8") as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)  # Extract only the first row as headers
                self.headers = headers
                print(f"Headers: {headers}")
                return headers
        except FileNotFoundError:
            print("Error: The file was not found.")
        except Exception as e:
            print(f"An unexpected error occurred while reading headers: {e}")
            return None


class CSVWriter:
    """
    A simple class to write rows of data to a CSV file in the user's Documents/Phase_One_Formatted_Data directory.
    """

    def __init__(self):
        """
        Initializes the CSVWriter class.
        Sets the output directory to:
           ~/Documents/Phase_One_Formatted_Data
        Creates this directory if it does not already exist.
        """
        # Get the path to the user's Documents directory
        downloads_directory = os.path.join(os.path.expanduser("~"), "Downloads")
        temp_output_directory = os.path.join(downloads_directory, "TAMU_Selfless_Service")
        self.output_directory = os.path.join(temp_output_directory, "Phase_Two_Formatted_Data")

        # Create the directory if it doesn't exist
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)
            print(f"Created directory: {self.output_directory}")

    def write_csv(self, rows, filename_suffix="output"):
        """
        Writes the provided rows of data to a CSV file, with a timestamped filename.

        :param rows: A list of lists. Each inner list represents one row for the CSV.
                     Example: [["Header1", "Header2"], ["Value1", "Value2"], ...]
        :param filename_suffix: A short string to include in the CSV filename.
        """
        try:
            # Create a timestamped filename (e.g. "2025-03-22__02_15 pm")
            timestamp = datetime.now().strftime("%Y-%m-%d__%I_%M %p").lower()
            filename = os.path.join(self.output_directory, f"{filename_suffix}_{timestamp}.csv")

            # Write the data to a CSV file
            with open(filename, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                for row in rows:
                    writer.writerow(row)

            print(f"CSV successfully written to: {filename}")

        except Exception as e:
            print(f"Failed to write CSV. Error: {str(e)}")
