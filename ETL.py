import pandas as pd
from datetime import datetime

# Extract phase
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

# Load JSON files
json_files = ['bank_market_cap_1.json', 'bank_market_cap_2.json']

# Extract data from JSON files
extracted_data = pd.DataFrame()
for json_file in json_files:
    extracted_data = extracted_data.append(extract_from_json(json_file), ignore_index=True)

# Extract exchange rates
exchange_rate_data = pd.read_csv('exchange_rates.csv', index_col=0)

# Transform phase
def transform(data):
    # Convert GBP to USD using the exchange rate
    exchange_rate = exchange_rate_data.loc['GBP', 'Rates']
    data['Market Cap (US$ Billion)'] = round(data['Market Cap (US$ Billion)'] * exchange_rate, 2)
    return data

transformed_data = transform(extracted_data)

# Load phase
def load(data_to_load, target_file):
    data_to_load.to_csv(target_file)

target_file = "bank_market_cap_gbp_to_usd.csv"
load(transformed_data, target_file)

# Log the end of the ETL process
print("ETL process ended")

# Log phases
def log(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(f"{timestamp}: {message}\n")

# Log messages
log("ETL Job Started")
log("Extract phase Ended")
log("Transform phase Ended")
log("Load phase Ended")
log("ETL Job Ended")
