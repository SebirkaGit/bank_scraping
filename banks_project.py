import sqlite3
from datetime import datetime 
import pandas as pd 
from bs4 import BeautifulSoup
import numpy as np 
import requests
import json



# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_format = '%Y-%b-%d-%H-%M-%S'
    now = datetime.now()
    str_now = now.strftime(time_format)
    with open("code_log.txt", "a") as log:
        log.write(f"{str_now} : {message}\n")


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    data_text = response.text
    data = BeautifulSoup(data_text, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            # Find all <a> tags in the second column
            bank_links = col[1].find_all('a')
            if bank_links:
                # The last <a> in the cell is the bank name
                bank_name = bank_links[-1].get_text(strip=True)
            else:
                bank_name = ""
            market_cap = col[2].get_text(strip=True)
            market_cap = col[2].get_text(strip=True)
            market_cap = float(market_cap.replace(",", ""))
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap  
            }
            # with open("finding_cols.txt", "a") as file:
            #     file.write(str(data_dict) + "\n")
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, exchange_rate_csv):
    # Read the exchange rate CSV file into a DataFrame
    exch_df = pd.read_csv(exchange_rate_csv)
    
    # Convert the DataFrame to a dictionary: {'EUR': 0.93, ...}
    exchange_rate = exch_df.set_index('Currency').to_dict()['Rate']
    
    # Add new columns, rounding to 2 decimal places
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


query_1 = 'SELECT * FROM Largest_banks'
query_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_3 = 'SELECT Name from Largest_banks LIMIT 5'
table_name = 'Largest_banks'
db_name = 'Largest_banks_Data.db'                        
from_csv_path = './exchange_rate.csv' 
to_csv_path = './df.csv' 
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]


# ETL Pipeline Execution
log_progress('Preliminaries complete. Initiating ETL process')  

df = extract(url, table_attribs)                        
log_progress('Data extraction complete. Initiating Transformation process')  

df = transform(df, from_csv_path)                                     
log_progress('Data transformation complete. Initiating loading process')  

load_to_csv(df, to_csv_path)                               
log_progress('Data saved to CSV file')                  

sql_connection = sqlite3.connect(db_name)               
log_progress('SQL Connection initiated.')               

load_to_db(df, sql_connection, table_name)              
log_progress('Data loaded to Database as table. Running the query')  

query_1 = 'SELECT * FROM Largest_banks'
query_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_1, sql_connection)              
run_query(query_2, sql_connection)              
run_query(query_3, sql_connection)              

log_progress('Process Complete.')                       

sql_connection.close()   
import sqlite3
from datetime import datetime 
import pandas as pd 
from bs4 import BeautifulSoup
import numpy as np 
import requests
import json



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_format = '%Y-%b-%d-%H-%M-%S'
    now = datetime.now()
    str_now = now.strftime(time_format)
    with open("code_log.txt", "a") as log:
        log.write(f"{str_now} : {message}\n")


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    data_text = response.text
    data = BeautifulSoup(data_text, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            # Find all <a> tags in the second column
            bank_links = col[1].find_all('a')
            if bank_links:
                # The last <a> in the cell is the bank name
                bank_name = bank_links[-1].get_text(strip=True)
            else:
                bank_name = ""
            market_cap = col[2].get_text(strip=True)
            market_cap = col[2].get_text(strip=True)
            market_cap = float(market_cap.replace(",", ""))
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap  
            }
            # with open("finding_cols.txt", "a") as file:
            #     file.write(str(data_dict) + "\n")
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, exchange_rate_csv):
    # Read the exchange rate CSV file into a DataFrame
    exch_df = pd.read_csv(exchange_rate_csv)
    
    # Convert the DataFrame to a dictionary: {'EUR': 0.93, ...}
    exchange_rate = exch_df.set_index('Currency').to_dict()['Rate']
    
    # Add new columns, rounding to 2 decimal places
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


query_1 = 'SELECT * FROM Largest_banks'
query_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_3 = 'SELECT Name from Largest_banks LIMIT 5'
table_name = 'Largest_banks'
db_name = 'Largest_banks_Data.db'                        
from_csv_path = './exchange_rate.csv' 
to_csv_path = './df.csv' 
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]


# ETL Pipeline Execution
log_progress('Preliminaries complete. Initiating ETL process')  

df = extract(url, table_attribs)                        
log_progress('Data extraction complete. Initiating Transformation process')  

df = transform(df, from_csv_path)                                     
log_progress('Data transformation complete. Initiating loading process')  

load_to_csv(df, to_csv_path)                               
log_progress('Data saved to CSV file')                  

sql_connection = sqlite3.connect(db_name)               
log_progress('SQL Connection initiated.')               

load_to_db(df, sql_connection, table_name)              
log_progress('Data loaded to Database as table. Running the query')  

query_1 = 'SELECT * FROM Largest_banks'
query_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_1, sql_connection)              
run_query(query_2, sql_connection)              
run_query(query_3, sql_connection)              

log_progress('Process Complete.')                       

sql_connection.close()   