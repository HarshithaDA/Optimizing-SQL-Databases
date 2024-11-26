# Import the connection function from sqlConnector.py
from sqlConnector import connectToDB
from queries import queries, hospitalsTableName, pricesTableName
import time

btreeIndexPricesQuery = f"""
        CREATE INDEX btreeIndex_prices ON {pricesTableName} (description(150), price, payer(200));
        """

bTreeIndexHospitalsQuery = f"""
CREATE INDEX btreeIndex_hospitals ON {hospitalsTableName} (beds, name(200));
"""

def createBTreeIndex():
   exec.execute(bTreeIndexHospitalsQuery)
   exec.execute(btreeIndexPricesQuery)

def fetchExecTime(query):
    startTime = time.time()
    exec.execute(query)
    exec.fetchall()
    endTime = time.time()
    return endTime - startTime

def printExecTime():
    queryNum = 1
    for query in queries:
     executionTime = fetchExecTime(query)
     print(f"Execution time for query {queryNum} is {executionTime}")
     queryNum += 1

dbConnect = connectToDB()
exec = dbConnect.cursor()

# Function to create btree index, comment out after initial run
createBTreeIndex()
printExecTime()

exec.close()
dbConnect.close()