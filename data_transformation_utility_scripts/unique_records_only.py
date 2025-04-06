# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 21:15:57 2025

@author: marcu
"""

import csv

def keep_unique_rows(input_csv, output_csv):
    seen = set()       # will hold tuples like (LastN, FirstN)
    unique_rows = []   # store the rows we want to keep

    # Read from input_csv
    with open(input_csv, 'r', newline='', encoding='utf-8') as infile:
        # If your file is truly tab-delimited, use delimiter='\t'
        reader = csv.DictReader(infile, delimiter=',')

        for row in reader:
            # Create a tuple key based on (LastN, FirstN)
            key = (row['LastN'], row['FirstN'])
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)

    # Write unique rows to output_csv
    with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        # Use the same fieldnames and delimiter as the input
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=',')

        # Write header row
        writer.writeheader()
        # Write only the unique rows we collected
        writer.writerows(unique_rows)

if __name__ == "__main__":
    input_path = "C:/Users/marcu/Downloads/Contact_Info.csv"
    output_path = "C:/Users/marcu/Downloads/Unique_Contact_Info.csv"
    keep_unique_rows(input_path, output_path)
    print(f"Unique rows have been written to {output_path}")