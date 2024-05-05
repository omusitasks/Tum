import os
import csv
import pyodbc

# Function to perform data cleansing
def cleanse_data(data):
    # Example: converting 'None' values to NULL
    for key, value in data.items():
        if value == 'None':
            data[key] = None
    return data

# Function to create table if it does not exist
def create_table(cursor, table_name, columns):
    # Check if table exists
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", table_name)
    if cursor.fetchone()[0] == 0:
        # Table does not exist, create it
        columns_def = ', '.join(['{} NVARCHAR(MAX)'.format(column) for column in columns if column != 'Id'])
        query = "CREATE TABLE {} (Id INT PRIMARY KEY IDENTITY(1,1), {})".format(table_name, columns_def)
        cursor.execute(query)


# Function to read CSV file and insert or update data into SQL Server database
# def process_csv_and_insert(csv_file, server, database, user, password):
def process_csv_and_insert(csv_file, server, database):
    # Connection string
    # conn_str = 'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+password

    # Connection string with Windows authentication
    conn_str = 'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get table name from CSV file
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    # Read CSV file
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        columns = csv_reader.fieldnames

        # Create table if not exists
        create_table(cursor, table_name, columns)

        # Insert data into SQL Server
        for row in csv_reader:
            # Cleanse data if needed
            row = cleanse_data(row)

            # Check if the row already exists
            cursor.execute("SELECT COUNT(*) FROM {} WHERE {} = ?".format(table_name, columns[0]), row[columns[0]])
            row_count = cursor.fetchone()[0]

            if row_count > 0:
                # If the row already exists, update it
                update_columns = ', '.join(['{} = ?'.format(col) for col in columns[1:]])
                query = "UPDATE {} SET {} WHERE {} = ?".format(table_name, update_columns, columns[0])
                cursor.execute(query, [row[col] for col in columns[1:]] + [row[columns[0]]])
                print("Data for '{}' updated in the database successfully.".format(row[columns[0]]))
            else:
                # If the row does not exist, insert it
                placeholders = ', '.join(['?'] * len(columns))
                insert_query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, ', '.join(columns), placeholders)
                cursor.execute(insert_query, list(row.values()))
                print("Data for '{}' inserted into the database successfully.".format(row[columns[0]]))

    # Commit and close connection
    conn.commit()
    conn.close()

# Main function
def main():
    # Configuration
    # csv_directory = 'C:/Users/user/Downloads/Data-Exercise#2/'
    csv_directory = 'C:/Users/user/Downloads/tum-new/Capstone Project- 1200sh- Data analytics/'
    server = 'DESKTOP-CUAKVNF'
    database = 'etl_pipeline_db'
    # user = 'user'
    # password = ''

    # Process each CSV file in the directory
    for filename in os.listdir(csv_directory):
        if filename.endswith('.csv'):
            csv_file = os.path.join(csv_directory, filename)
            process_csv_and_insert(csv_file, server, database)
            # process_csv_and_insert(csv_file, server, database, user, password)

if __name__ == "__main__":
    main()
