import requests
from datetime import date, timedelta
from pymongo import MongoClient, ASCENDING
from datetime import datetime, timedelta

# url containing futures data
data_url = "http://sgx.com/JsonRead/JsondtData?qryId=DBO.FEQUITYINDEX&%20noCache=1520589111250.358046.5664677842"
# url containing trading hours
date_url = "http://sgx.com/JsonRead/JsondtData?qryId=DBO.L.DNTitle&timeout=60&%20noCache=1521175405278.1063942.5158965744"
contract = ["sgp", "tw", "cn"]
data_labels = {"CC":"contract", "MY":"contract_month", "LTP":"last_trade_price", "TH": "high", "TL": "low", "LUT": "last_updated",
          "CTS":"trade_session"}
date_labels = {"CC":"contract", "TH":"trading_hours", "TH1":"trading_hours_1"}
database = "sgx_tracker"

# query and collect listings for a given url and returns an array of each row of data
def scrap(url):
    try:
        # request data
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        c = r.content.decode('utf-8')
        # trim header information
        c_trimmed = c.split('[')[1]
        # creates rows of data with leading '{' that needs to be stripped later
        rows = c_trimmed.split('},')
        # strips '{'
        for idx, item in enumerate(rows):
            rows[idx] = item[1:]
        return rows

    except requests.exceptions.RequestException:
        print("CONNECTION FAILED")

# format data
def format_data(data, filter):
    current_date = datetime.now()
    formatted_data = []
    for row in data:
        properties = row.split(',')
        return_row = {}
        for p in properties:
            k = p.split(':', 1)[0]
            v = p.split(':', 1)[1]
            if k in filter:
                # format key to be more descriptive
                _k = filter[k]
                # strip excess quotes
                v = v.strip("'")
                # convert prices to float value
                if _k == "last_trade_price" or _k == "high" or _k == "low":
                    # if field is not blank
                    if v != "-":
                        v = float(v[:-2]+'.'+v[-2:])
                # convert trading hours to 24 hour format
                if _k == "trading_hours" or _k == "trading_hours_1":
                    v = v.strip("SGX (T+1) Trading Hours: Mon - Fri") 
                    v = v.split("-")
                    _v = ["", ""]
                    for counter, value in enumerate(v):
                        if "am" in value:
                            value = value.strip("am pm")
                            value = value.split(":")
                            value = "".join([value[0].zfill(2), ':', value[1]])
                            _v[counter] = value
                        elif "pm" in value:
                            value = value.strip("am pm")
                            value = value.split(":")
                            value = "".join([str(int(value[0])+12), ':', value[1]])
                            _v[counter] = value
                    v = _v
                return_row[_k] = v 
        # add date tracked to each row
        return_row["date_tracked"] = current_date
        formatted_data.append(return_row)
    return formatted_data

# returns MongoClient collection
def get_db(database, collection, username, password, port="27017", host="localhost"):
    uri = "mongodb://{}:{}@{}:{}".format(username, password, host, port)
    client = MongoClient(uri)
    db = client[database]
    collection = db[collection]
    return collection

# get current datetime
current_datetime = datetime.now()
current_year = current_datetime.year
current_month = current_datetime.month
current_day = current_datetime.day

# scrap data
data = scrap(data_url)

# formats data
data = format_data(data, data_labels)

# get mongo auth credentials
with open ("password.txt", 'r') as pw:
    global username, password
    read = pw.readline().split(":")
    username = read[0]
    password = read[1]
    print(username, password)

# get mongo collections
db_data = get_db(database, "data", username, password)
db_date = get_db(database, "date", username, password)

# populate db_date if it is empty
if db_date.find_one() == None:
    dates = scrap(date_url)
    dates = format_data(dates, date_labels)
    db_date.insert_many(dates)

# add data to db
currently_open = set() # for testing
# trading work days are between Monday to Friday
if current_datetime.weekday() <= 5:
    for d in data:
        date = db_date.find_one({"contract": d["contract"]})
        query = {"contract": d["contract"], "trade_session": d["trade_session"], "contract_month": d["contract_month"]}
        to_push = {"last_trade_price": d["last_trade_price"], "low": d["low"], "high": d["high"], "date_tracked": d["date_tracked"]}
        # convert trading_hours to datetime format
        trading_hours = [datetime(current_year, current_month, current_day, 
                                int(time.split(":")[0]), int(time.split(":")[1]))
                                for time in date["trading_hours"]] 
        trading_hours_1 = [datetime(current_year, current_month, current_day, 
                                int(time.split(":")[0]), int(time.split(":")[1]))
                                for time in date["trading_hours_1"]]
        # add offset for closing hours in the next day
        if trading_hours[1] < trading_hours[0]:
            trading_hours[1] += timedelta(hours=24)
        if trading_hours_1[1] < trading_hours_1[0]:
            trading_hours_1[1] += timedelta(hours=24)
        # add data to db if current time is within trading hours
        if trading_hours[0] < current_datetime < trading_hours[1] or\
        trading_hours_1[0] < current_datetime < trading_hours_1[1]:
            currently_open.add(d["contract"])
            if db_data.find_one(query) is not None:
                    # update document if it already exists
                    db_data.update(query, {"$push": to_push})
            else:
                    # create document if it does not exist
                    d["high"] = [d["high"]]
                    d["low"] = [d["low"]]
                    d["last_trade_price"] = [d["last_trade_price"]]
                    d["date_tracked"] = [d["date_tracked"]]
                    d["daily"] = {}
                    db_data.insert_one(d)
        # save daily high, low and last_trade_prices if outside of trading hours
        else:
            current_date = str(current_datetime.day) + "-" + str(current_datetime.month) + "-" + str(current_datetime.year)
            to_push["date"] = current_date
            db_data.update(query, {"$set": {"daily": to_push}})

print(currently_open)
print(len(currently_open))



