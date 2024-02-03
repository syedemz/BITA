import csv
import json
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine

def csvReader():
    schema = []
    with open('Stock.CSV', 'r', encoding='utf-8-sig') as file:
        # Using DictReader to infer column types
        reader = csv.DictReader(file, delimiter=';')

        if reader.fieldnames:
            print("Column Names and Types:")
            # Reading only the first row
            first_row = next(reader)

            for column_name, value in first_row.items():
                # Inferring data type based on the value
                data_type = infer_data_type(value)
                print(f"{column_name}: {data_type}")
                schema.append([str(column_name), data_type])

            return schema
        else:
            print("No data in the CSV file or missing header.")
            

def infer_data_type(column_values):
    # Your logic to infer data type can be more sophisticated
    # For simplicity, this example checks for integer and float
    if all(value.isdigit() for value in column_values):
        return "Integer"
    elif all(value.replace('.', '', 1).isdigit() for value in column_values):
        return "Float"
    else:
        return "String"


def bulk_import_alchemy(user, password, database, schema, tablename):

    print("importing data using sqlalchemy")

    try:
        # Load CSV into a Pandas DataFrame
        df = pd.read_csv("Stock.CSV")


        connection_string = f"postgresql://{user}:{password}@localhost:5432/{database}"
        engine = create_engine(connection_string)
        table_name = f"{schema}.{tablename}"

        # Write DataFrame to PostgreSQL
        df.to_sql(table_name, engine, if_exists='append', index=False)

        print(f"Data successfully uploaded to table {table_name}.")
    except Exception as e:
        print(f"Error: {e}")


def check_previous_import():
    with open('previousImport.json', 'r') as file:
        data = json.load(file)
    previous_import_date = data["previousImport"]["dateString"]
    if(previous_import_date):
        print("previous import has been made")
        return True
    else:
        print("no previous import has been made")
        return False


def set_import_date():
    current_datetime = datetime.now()
    current_date_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

    new_data = {
        "previousImport": {
            "dateString": current_date_string
        }
    }
    with open('previousImport.json', 'w') as file:
        json.dump(new_data, file, indent=2)

    print("Import time has been successfully updated.")



def bulk_import(host, port, database, user, password, schema, table, method = 'pgloader'):
    DELIMITER = ";"
    CSV_FILE = "Stock.CSV"
    #command = f"pgloader {host}:{port} {database} {user} {password} --with batch_size=5000 --with {schema} --table {table} --delimiter '{DELIMITER}' {CSV_FILE}"
    if(method == 'pgloader'):
        command = "pgloader csv.load"  
        print(command)
        try:
            os.system(command)
            print(f"Data from csv file successfully loaded into table.")
        except Exception as e:
            print(f"Error: {e}")
    else:
        bulk_import_alchemy(user, password, database, schema, table)









