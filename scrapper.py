import requests
from datetime import date, timedelta
import csv
from pandas import DataFrame

url = "http://sgx.com/JsonRead/JsondtData?qryId=DBO.FEQUITYINDEX&%20noCache=1520589111250.358046.5664677842"
contract = ["sgp"]
# contract = ["sgp", "tw", "cn"]
csv_file = "futures.xlsx"
labels = ["CC", "MY", "LTP", "TH", "TL"]

# query and collect listings for a given URL and returns an array of each row of data
def scrap(url):
    try:
        # request data
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        c = r.content.decode('utf-8')
        c_trimmed = c.split('[')[1]
        # creates rows of data with leading '{' that needs to be stripped later
        rows = c_trimmed.split('},')
        # strips '{'
        for idx, item in enumerate(rows):
            rows[idx] = item[1:]
        return rows

    except requests.exceptions.RequestException:
        print("Connection failed")


# filter rows based on relevant contracts and desired output
def filter_data(data, input_filter, output_filter):
    filtered_data = {}
    for i in input_filter:
        # creates an array of values for each property for each contract
        filtered_data[i] = {}
        for o in output_filter:
            filtered_data[i][o] = [] 
    for row in data:
        # check if row is of desired contract 
        if "CC:'{}'".format(i.upper()) in row:
            # separates properties in row
            properties = row.split(',')
            for p in properties:
                k = p.split(':')[0]
                v = p.split(':')[1]
                # if key is in desired output, add to new_row
                if k in output_filter:
                    filtered_data[i][k].append(v)
    return filtered_data

# save output into external csv
def save_data(data, file):
    for contract in data:
        df = DataFrame(data[contract])
        df.to_excel(file, sheet_name=contract, index=False)

    # for row in data:

    # df = DataFrame



scrapped = scrap(url)
filtered = filter_data(scrapped, contract, labels)
save_data(filtered, csv_file)


