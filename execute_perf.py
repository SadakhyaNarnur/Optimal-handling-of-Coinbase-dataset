from symtable import Symbol
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import time

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
data = pd.read_csv('/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/load.csv', dtype= {'unix':'int32','symbol':'string', 'low':'float32', 'high':'float32', 'open':'float16', 'close':'float16','volume':'float32'})
try:
    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])

    # Insert DataFrame records one by one.
    for i,row in data.iterrows():
        sql = "INSERT INTO `coinbase_transactions_min` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        mydb.commit()

except (mysql.connector.Error,mysql.connector.Warning) as e:
    mydb.close()
    print(e)

after = time.time()
print("Bulk load using execute time taken in secs:",after-before)