from data_transformation.constants import required_headers #list
from data_transformation.constants import contact_list_headers
from data_transformation.constants import output_headers #list
from data_transformation.csv_handler import CSVHandler #class
from data_transformation.data_transformation import AddressParser
from data_transformation.csv_handler import CSVWriter #class
from data_transformation.csv_handler import MissingHeaderException #exception class\
from data_transformation.data_transformation import DatabaseConnector #class
from datetime import datetime
import sys
import sqlite3


# Prompt the user to select the CSV to be used for transformed.
try:
    print("Select the Target CSV File")
    csv_handler = CSVHandler()
    csv_handler.read() # Read the selected CSV file
    records = csv_handler.get_records() # Load the records variable to have the content
    csv_handler.ensure_headers_exist(required_headers) # Ensures the expected headers are present in CSV file.

    # Initialize lists to store log messages
    csv_writer = CSVWriter()
    # Define output_row list, assign headers:
    output_row = [output_headers]
except FileNotFoundError as e:
    print(f"{e}")
    sys.exit()
except MissingHeaderException as e:
    print(f"\n{e}\nEnsure column headers are spelled and formatted exactly as required.") #skip line, error message, skip line
    sys.exit()

# Clean the Address Column in-memory.
try:
    # 1) Create an AddressParser instance
    address_parser = AddressParser()

    # 2) Loop over each row and transform the "Address" column
    for index, row in enumerate(records):
        original_address = row.get("Address", "")
        transformed_address = address_parser.transform_address(original_address)
        row["Address"] = transformed_address  # Overwrite with new, cleaned address

    print("Successfully transformed Address column in-memory.")

except Exception as e:
    print(f"{e}")
    sys.exit(1)


try:
    database_conn = DatabaseConnector()
    database_conn.initialize_database()
    database_conn.check_and_drop_table()
    database_conn.create_table()
except sqlite3.Error as e:
    print(f"Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)


try:
    print("Select the Contact Info CSV File")
    contact_list_csv_handler = CSVHandler()
    contact_list_csv_handler.read()
    contact_list_records = contact_list_csv_handler.get_records()  # Load the records variable to have the content
    contact_list_csv_handler.ensure_headers_exist(contact_list_headers)  # Ensures the expected headers are present in CSV file.
    for index, row in enumerate(contact_list_records):
        original_address = row.get("Address", "")
        row["Address"] = original_address.title()  # Overwrite with new, title() address
except FileNotFoundError as e:
    print(f"{e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)


try:
    for index, row in enumerate(contact_list_records):
        database_conn.insert_record(
            row["Donor_ID"],
            row["Last_Name"],
            row["First_Name"],
            row["Address"],
            row["City"],
            row["State"],
            row["Zipcode"],
            row["Phone"],
            row["Email"]
        )
except sqlite3.Error as e:
    print(f"Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)


output_records = []

try:
    for index, row in enumerate(records):
        # Debug: show row index and raw data
        print(f"\nDebug: Processing row {index}: {row}")

        # 1) Full name
        raw_fullname = str(row.get("Name")).strip().lower()
        print(f"Debug: raw_fullname='{raw_fullname}'")

        # Assign output_variable
        output_middle_name = ""

        # 2) Check if organization
        org_keywords = ["church", "fellow", "frat", "fraternity",
                        "foundation", "llc", "bcs", "club",
                        "agency", "firm"]

        if any(keyword in raw_fullname for keyword in org_keywords):
            raw_organization = raw_fullname
            raw_fullname = None
            print(f"Debug: Marked as organization => raw_organization='{raw_organization}'")
        else:
            raw_organization = ""
            tokenized_name = raw_fullname.split("|")
            print(f"Debug: tokenized_name={tokenized_name}")

            # Safely extract last_name, first_name
            if len(tokenized_name) == 2:
                last_name = tokenized_name[0].strip()
                first_name = tokenized_name[1].strip()
            elif len(tokenized_name) == 1:
                last_name = tokenized_name[0].strip()
                first_name = ""
            else:
                last_name = ""
                first_name = ""

            # Final assignment
            output_last_name = last_name.capitalize()
            output_first_name = first_name.capitalize()
            print(f"Debug: Person => last_name='{output_last_name}', first_name='{output_first_name}'")

        # 3) Read the rest
        raw_date = str(row.get("Date")).strip()
        raw_amount = str(row.get("Amount")).strip()
        raw_fund = str(row.get("Fund")).strip().lower()
        raw_account = str(row.get("Account")).strip().capitalize()
        raw_campaign = str(row.get("Campaign")).strip()
        raw_appeal = str(row.get("Appeal")).strip()
        raw_payment_method = str(row.get("Method")).strip()
        raw_address = str(row.get("Address")).strip()
        print(f"Debug: raw_date='{raw_date}', raw_amount='{raw_amount}', raw_address='{raw_address}'")

        # 4) Parse address safely
        if "EMPTY" in raw_address or "INCORRECT DATA" in raw_address:
            street = city = state = zipcode = ""
        else:
            address_tokens = raw_address.split("|")
            print(f"Debug: address_tokens={address_tokens}")
            street  = address_tokens[0].strip() if len(address_tokens) >= 1 else ""
            city    = address_tokens[1].strip() if len(address_tokens) >= 2 else ""
            state   = address_tokens[2].strip() if len(address_tokens) >= 3 else ""
            zipcode = address_tokens[3].strip() if len(address_tokens) >= 4 else ""

        # 5) Actually figure out whether it’s a person or org in final output
        if raw_fullname is not None:
            # If we are in the person scenario
            output_first_name = first_name.title()
            output_last_name = last_name.title()
        else:
            # It's an organization
            output_first_name = ""
            output_last_name = ""

        # 6) Suffix, Title, etc.
        output_suffix = ""
        output_title = ""
        if raw_fullname is None:
            output_organization = raw_organization.title()
        else:
            output_organization = ""

        # 7) Check DB for existing contact
        boolean_person_exists = False
        boolean_organization_exists = False

        # Make sure to handle blank names
        if output_last_name or output_first_name:
            boolean_person_exists = database_conn.query_for_match_by_name(output_last_name, output_first_name)
        if not boolean_person_exists and output_organization:
            boolean_organization_exists = database_conn.query_for_match_by_name(raw_organization, raw_organization)

        print(f"Debug: boolean_person_exists={boolean_person_exists}, boolean_organization_exists={boolean_organization_exists}")

        # 8) Home Address
        if boolean_person_exists:  # retrieve from DB
            output_home_address = database_conn.query_for_address(output_last_name, output_first_name)
            print(f"Debug: retrieved address from DB => {output_home_address}")
        else:
            output_home_address = street

        if boolean_organization_exists:  # retrieve from DB
            output_home_address = database_conn.query_for_address(output_organization, output_organization)
            print(f"Debug: org address from DB => {output_home_address}")
        else:
            # if the prior `else` replaced it, we may keep it
            pass

        # 9) City
        if boolean_person_exists:
            output_home_city = database_conn.query_for_city(output_last_name, output_first_name)
        else:
            output_home_city = city
        if boolean_organization_exists:
            output_home_city = database_conn.query_for_city(output_organization, output_organization)

        # 10) State
        if boolean_person_exists:
            output_home_state = database_conn.query_for_state(output_last_name, output_first_name)
        else:
            output_home_state = state
        if boolean_organization_exists:
            output_home_state = database_conn.query_for_state(output_organization, output_organization)

        # 11) Zip
        if boolean_person_exists:
            output_home_zipcode = database_conn.query_for_zipcode(output_last_name, output_first_name)
        else:
            output_home_zipcode = zipcode
        if boolean_organization_exists:
            output_home_zipcode = database_conn.query_for_zipcode(output_organization, output_organization)

        # 12) Phone
        if boolean_person_exists:
            output_phone1 = database_conn.query_for_phone(output_last_name, output_first_name)
        else:
            # originally address_tokens[2], but that might be out of range
            output_phone1 = ""
        if boolean_organization_exists:
            output_phone1 = database_conn.query_for_phone(output_organization, output_organization)
        else:
            pass
        output_phone2 = ""

        # 13) Email
        if boolean_person_exists:
            output_email = database_conn.query_for_email(output_last_name, output_first_name)
        else:
            output_email = ""

        # 14) Date
        try:
            date_object = datetime.strptime(raw_date, "%m/%d/%Y")
            output_date = date_object.strftime("%m/%d/%Y")
        except ValueError as ve:
            # If the date isn't in that format, let's see the error
            print(f"Debug: date parsing error => {ve}")
            output_date = ""

        # 15) Amount
        output_amount = raw_amount.replace("$", "")

        # 16) Fund, Campaign, etc.
        output_fund = raw_fund.title()
        output_campaign = raw_campaign.title()
        output_appeal = raw_appeal
        output_method = raw_payment_method.capitalize()

        # 19) Acknowledged?
        output_acknowledged = ""
        output_note = ""

        # Build final row
        output_row = [
            output_title,
            output_first_name,
            output_middle_name,
            output_last_name,
            output_suffix,
            output_organization,
            output_home_address,
            output_home_city,
            output_home_state,
            output_home_zipcode,
            output_phone1,
            output_phone2,
            output_email,
            output_date,
            output_amount,
            output_fund,
            output_campaign,
            output_appeal,
            output_method,
            output_acknowledged,
            output_note
        ]

        # Debug: show final row
        print(f"Debug: Final output row => {output_row}")

        # Append to output_records
        output_records.append(output_row)

    # Write the log to a CSV file
    try:
        csv_writer.write_csv(output_records, filename_suffix="2021_RAW")
        print(f"Writing out to {csv_writer.output_directory}")
    except Exception as e:
        print(f"Failed to write output log. Error: {str(e)}")

except Exception as e:
    sys.exit()