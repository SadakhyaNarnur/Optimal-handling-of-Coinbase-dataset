import mysql.connector
from mysql.connector import errorcode
import time

try:
    mydb = mysql.connector.connect(
        host = "db-project.csmblagoongs.us-east-1.rds.amazonaws.com",
            user = "admin",
            password = "dbproject_deploy",
            database = "coinbase") 
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
cursor = mydb.cursor()
before = time.time()
cursor.execute("SELECT count(1), year, month FROM coinbase_transactions_min group by year, month")
after = time.time()
data = cursor.fetchall()
print(data)
print("Time taken for executing the query:",after-before)