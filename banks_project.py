import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from config import LOG_PATH, URL, TABLE_ATTRIBS_BEFORE, CSV_EXCHANGE_PATH, CSV_PATH

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_PATH, 'a') as f:
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch data")
    data = BeautifulSoup(response.text, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    data_list = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            data_list.append({
                'Name': cols[1].text.strip(),
                'MC_USD_Billion': cols[2].text.strip(),
            })
    
    df = pd.DataFrame(data_list, columns=table_attribs)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
	information, and adds three columns to the data frame, each
	containing the transformed version of Market Cap column to
	respective currencies'''

    rates_df = pd.read_csv(csv_path)

    rate_eur = rates_df.loc[rates_df['Currency'] == 'EUR', 'Rate'].values[0]
    rate_gbp = rates_df.loc[rates_df['Currency'] == 'GBP', 'Rate'].values[0]
    rate_inr = rates_df.loc[rates_df['Currency'] == 'INR', 'Rate'].values[0]

    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '').astype(float)

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * rate_gbp, 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * rate_eur, 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * rate_inr, 2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
	the provided path. Function returns nothing.'''

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(URL, TABLE_ATTRIBS_BEFORE)
log_progress("Data extraction complete. Initiating Transformation process")
print("Extracted DataFrame:")
print(df.head())

df = transform(df, CSV_EXCHANGE_PATH)
log_progress("Data transformation complete. Initiating Loading process")
print("\nTransformed DataFrame:")
print(df.head())

load_to_csv(df, CSV_PATH)
log_progress("Data saved to CSV file")
print(f"\nData saved to CSV at: {CSV_PATH}")

