import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('olist_customers_dataset.csv', 'olist_customers_dataset'),
    ('olist_orders_dataset.csv', 'olist_orders_dataset'),
    ('olist_sellers_dataset.csv', 'olist_sellers_dataset'),
    ('olist_products_dataset.csv', 'olist_products_dataset'),
    ('olist_geolocation_dataset.csv', 'olist_geolocation_dataset'),
    ('olist_order_payments_dataset.csv', 'olist_order_payments_dataset'),  # Added payments.csv for specific handling
    ('olist_order_items_dataset.csv', 'olist_order_items_dataset'),
    ('olist_order_reviews_dataset.csv', 'olist_order_reviews_dataset'),
    ('product_category_name_translation.csv', 'product_category_name_translation')
    
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='12345',
    database='EcommerceBrazil'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'C:/Users/Vikash/Desktop/python file/EcommerceBrazil'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()