# Import the connection function from sqlConnectorRangePartitioning.py
from sqlConnectorRangePartitioning import connectToDB
from queries import queries
import time


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

printExecTime()

exec.close()
dbConnect.close()
