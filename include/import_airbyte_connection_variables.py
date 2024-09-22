import json
from airflow.models import Variable
import os

def import_variables_from_json(file_path):
    """Import variables from a JSON file into Airflow."""
    with open(file_path, 'r') as file:
        variables = json.load(file)
        for company, connections in variables.items():
            for key, value in connections.items():
                # Construct a variable name that includes the company for clarity
                variable_name = f"{company.upper()}_{key}"
                Variable.set(variable_name, value)
                print(f"Set variable: {variable_name} with value: {value}")

# Path to your JSON file
file_path = os.path.join('/usr/local/airflow/include', 'airbyte_variables.json')  # Updated file name

# Run the import function
import_variables_from_json(file_path)
