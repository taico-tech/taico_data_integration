import pandas as pd
import sys

def transform_csv(input_file, output_file, column_name):
    # Read the input CSV file
    df = pd.read_csv(input_file)

    # Get distinct values from the specified column
    distinct_values = df[[column_name]].drop_duplicates()

    # Write the result to the output CSV file
    distinct_values.to_csv(output_file, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python transform_csv.py <input_file> <output_file> <column_name>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    column_name = sys.argv[3]

    transform_csv(input_file, output_file, column_name)
    print(f"Distinct values from column '{column_name}' have been written to '{output_file}'.")
