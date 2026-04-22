# Code for ETL operations on Country-GDP data

# Importing the required libraries
# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# Declaring known variables
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "Market Cap (US$ Billion)"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'
exchange_rate_csv_path = './exchange_rate.csv'
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')
    
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            if col[1].find('a') is not None and len(col[1].find_all('a')) > 1:
                market_cap_str = col[2].contents[0]
                market_cap_val = float(market_cap_str[:-1])
                
                data_dict = {"Name": col[1].find_all('a')[1]['title'],
                             "Market Cap (US$ Billion)": market_cap_val}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    # Read exchange rate CSV file
    exchange_rate = pd.read_csv(csv_path)
    
    # Convert to dictionary: {'EUR':0.93, 'GBP':0.8, 'INR':82.95}
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    
    # Add 3 new columns and round to 2 decimal places
    df['Market Cap (EUR Billion)'] = np.round(df['Market Cap (US$ Billion)'] * exchange_rate['EUR'], 2)
    df['Market Cap (GBP Billion)'] = np.round(df['Market Cap (US$ Billion)'] * exchange_rate['GBP'], 2)
    df['Market Cap (INR Billion)'] = np.round(df['Market Cap (US$ Billion)'] * exchange_rate['INR'], 2)
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
	the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
	table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    print(query_statement)  # Lab me bola hai query bhi print karni hai
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print()  # empty line for clean output

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Executing ETL process

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, exchange_rate_csv_path)
log_progress("Data transformation complete. Initiating loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

log_progress("Initiating SQLite connection")
sql_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, 'Largest_banks')
log_progress("Data loaded to Database as a table, Connection closed")



log_progress("Query 1: Printing entire table")
query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_connection)

log_progress("Query 2: Average Market Cap in GBP")
query2 = "SELECT AVG(`Market Cap (GBP Billion)`) FROM Largest_banks"
run_query(query2, sql_connection)

log_progress("Query 3: Top 5 banks")
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query3, sql_connection)
 

sql_connection.close()
print("ETL Job Completed Successfully")

df = transform(df, 'exchange_rate.csv')
print(df)   # <-- Ye line add kar de
