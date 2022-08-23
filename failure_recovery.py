import json
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import time

import requests

def writeToFile(path):
    try:
        file = open(path,'a') 
        addToFile = "Local has data "
        file.write(addToFile)
        file.close()
    except:
        print('Something went wrong!')

def deleteFromFile(path):
    try:
        file = open(path,'w') 
        file.truncate()
        file.close()
    except:
        print('Something went wrong!')

def readFromFile(path):
    try:
        file = open(path,'r')
        with file as ins:
            count = 0
            for line in ins:
                count += 1
        file.close()
    except:
        print('Something went wrong!')

    if count > 0:
        try:
            new = mysql.connector.connect(
                host = "localhost",
                user = "root",
                password = "Qwerty12#",
                database = "db_project")
            table_name = "coinbase"
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        cursor = new.cursor()
        query = "SELECT * FROM "+table_name+""
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall() , columns = ["unix","symbol","low","high","open","close","volume","date","vol_fiat"])
        delete_query = "TRUNCATE TABLE "+table_name+""
        cursor.execute(delete_query)
        new.close()
        try:
            mydb = mysql.connector.connect(
                host = "db-project.csmblagoongs.us-east-1.rds.amazonaws.com",
                user = "admin",
                password = "dbproject_deploy",
                database = "coinbase") 
            table_name = "coinbase_transactions_min"
            cursor = mydb.cursor()
            # creating column list for insertion
            cols = "`,`".join([str(i) for i in df.columns.tolist()])
            # Insert DataFrame records one by one.
            for i,row in df.iterrows():
                sql = "INSERT INTO "+table_name +" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))

                # the connection is not autocommitted by default, so we must commit to save our changes
                mydb.commit()
            deleteFromFile("/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/checkFailure.txt")
        except (mysql.connector.Error,mysql.connector.Warning) as e:
            print(e)
        

try:
    mydb = mysql.connector.connect(
        host = "db-project.csmblagoongs.us-east-1.rds.amazonaws.com",
        user = "admin",
        password = "dbproject_deploy",
        database = "coinbase") 
    table_name = "coinbase_transactions_min"
    readFromFile("/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/checkFailure.txt")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    elif err.errno == errorcode.CR_CONN_HOST_ERROR:
        mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Qwerty12#",
        database = "db_project")
        table_name = "coinbase"
        writeToFile("/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/checkFailure.txt")
    else:
        print(err)
cursor = mydb.cursor()
before = time.time()
def fetch_daily_data(symbol):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol = pair_split[0] + '-' + pair_split[1]
    url = f'https://api.pro.coinbase.com/products/{symbol}/candles?granularity=60'
    response = requests.get(url)
    if response.status_code == 200: 
        data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
        data['symbol'] = symbol
        data['date'] = pd.to_datetime(data['unix'], unit='s')  # convert to a readable date
        data['vol_fiat'] = data['volume'] * data['close']      # multiply the BTC volume by closing price to approximate fiat volume
        data['year'] = pd.to_datetime(data['date']).dt.year
        data['month'] = pd.to_datetime(data['date']).dt.month

        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for this symbol")
        else:
            max_output = "SELECT MAX(date) from secondary_table where symbol = \"{}\"".format(symbol)
            cursor.execute(max_output)
            max_timestamp = cursor.fetchone()
            data_new = data[(data['date']> max_timestamp[0])]
            
            # creating column list for insertion
            cols = "`,`".join([str(i) for i in data_new.columns.tolist()])

            # Insert DataFrame records one by one.
            for i,row in data_new.iterrows():
                sql = "INSERT INTO `secondary_table` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                # the connection is not autocommitted by default, so we must commit to save our changes
                mydb.commit()
    else:
        print("Did not receieve OK response from Coinbase API")

if __name__ == "__main__":
# we set which pair we want to retrieve data for
    pair = ["BTC/USD"]
    for x in pair:
        fetch_daily_data(symbol=x)

after = time.time()
print(after-before)