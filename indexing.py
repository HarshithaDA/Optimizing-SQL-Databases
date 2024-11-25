# Import the connection function from sqlConnector.py
from sqlConnector import connectToDB
from queries import queries, hospitalsTableName, pricesTableName
import time

btreeIndexPricesQuery = f"""
        CREATE INDEX btreeIndex ON {pricesTableName} (description(150), price, payer(200));
        """

bTreeIndexHospitalsQuery = f"""
"""

#def createBTreeIndex():
#    exec.execute(bTreeIndexHospitalsQuery)
#   exec.execute(btreeIndexPricesQuery)

def fetchExecTime(query):
    startTime = time.time()
    exec.execute(query)
    exec.fetchall()
    endTime = time.time()
    return endTime - startTime

queryNum = 1

dbConnect = connectToDB()
exec = dbConnect.cursor()

#createBTreeIndex()

for query in queries:
     executionTime = fetchExecTime(query)
     print(f"Exceution time for query {queryNum} is {executionTime}")
     queryNum += 2

exec.close()
dbConnect.close()