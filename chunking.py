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
    table_name = "coinbase_transactions_min"
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
cursor = mydb.cursor()
before = time.time()
chunk_size=100000
batch_no=1
for chunk in pd.read_csv('//Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/load.csv',chunksize=chunk_size, dtype= {'unix':'int32','symbol':'string', 'low':'float32', 'high':'float32', 'open':'float16', 'close':'float16','volume':'float32'}):
    try:
      chunk_time1 = time.time()
      tpls = [tuple(x) for x in chunk.to_numpy()]
      # creating column list for insertion
      cols = ','.join(list(chunk.columns))

      # Insert DataFrame records one by one.
      sql = "insert into coinbase_transactions_min(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"%(cols)
      cursor.executemany(sql,tpls)
      chunk_time2 = time.time()
      print("Chunk no.: ", batch_no)
      print("Time taken in sec:", chunk_time2 - chunk_time1 )
      # the connection is not autocommitted by default, so we must commit to save our changes
      mydb.commit()
      batch_no+=1
    except (mysql.connector.Error,mysql.connector.Warning) as e:
        mydb.close()
        print(e)

after = time.time()
print("Data loading as chunks in secs",after-before)