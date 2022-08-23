import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import time
import numpy as np

try:
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Qwerty12#",
        database = "project") 
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
cursor = mydb.cursor()
before = time.time()
data = pd.read_csv('/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/load.csv')
try:
    tpls = [tuple(x) for x in data.to_numpy()]
    # creating column list for insertion
    cols = ','.join(list(data.columns))

    # Insert DataFrame records one by one.
    sql = "insert into coinbase_transactions_min(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"%(cols)
    cursor.executemany(sql,tpls)
    # the connection is not autocommitted by default, so we must commit to save our changes
    mydb.commit()

except (mysql.connector.Error,mysql.connector.Warning) as e:
    mydb.close()
    print(e)

after = time.time()
print("Bulk loading using executemany time taken in secs:",after-before)