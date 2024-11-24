# Import the connection function from sqlConnector.py
from sqlConnector import connectToDB
from queries import queryOne, queryTwo, queryThree, queryFour, queryFive
import time

pricesTableName = "PRICES"
btreeIndexQuery = f"""
        CREATE INDEX btreeIndex ON {pricesTableName} (description(150), price);
        """

dbConnect = connectToDB()
exec = dbConnect.cursor()
#exec.execute(btreeIndexQuery)
print("Created index on desc and price")
startTime = time.time()
exec.execute(queryOne)
endTime = time.time()
print("Run time: ",endTime - startTime)
result = exec.fetchall()
# for row in result:
#     print(row)
exec.close()
dbConnect.close()