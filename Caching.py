import time
from sqlConnector import connectToDB
from queries import queries

# Query to check if a query exists in the cache
checkCacheQuery = f"""
SELECT execution_time FROM query_cache WHERE query = %s;
"""

# Query to insert a query and its execution time into the cache
insertCacheQuery = f"""
INSERT INTO query_cache (query, execution_time) VALUES (%s, %s)
ON DUPLICATE KEY UPDATE execution_time = VALUES(execution_time), last_updated = CURRENT_TIMESTAMP;
"""

def fetchFromCache(query):
    # Check if the query is in the cache
    checkCacheQuery = f"SELECT execution_time FROM query_cache WHERE query = %s"
    
    # Start timer for cache lookup
    cacheStartTime = time.time()
    exec.execute(checkCacheQuery, (query,))
    cachedResult = exec.fetchone()
    cacheEndTime = time.time()

    if cachedResult:
        # Cache hit: Report the time to retrieve the cached result
        cacheFetchTime = cacheEndTime - cacheStartTime
        print(f"Cache hit for query: {query}")
        print(f"Time to retrieve from cache: {cacheFetchTime:.4f} seconds")
        print(f"Cached execution time: {cachedResult[0]:.4f} seconds")
        return cacheFetchTime  # Return the time to retrieve from cache
    else:
        # Cache miss: Execute query and measure its execution time
        print(f"Cache miss for query: {query}")
        startTime = time.time()
        exec.execute(query)
        exec.fetchall()  # Retrieve all results
        endTime = time.time()
        executionTime = endTime - startTime

        # Store the query and execution time in the cache
        storeCacheQuery = f"INSERT INTO query_cache (query, execution_time) VALUES (%s, %s)"
        exec.execute(storeCacheQuery, (query, executionTime))
        dbConnect.commit()  # Commit the transaction
        print(f"Query executed in {executionTime:.4f} seconds and cached.")
        return executionTime  # Return the actual execution time

def printExecTime():
    """Prints execution time for each query in the list."""
    queryNum = 1
    for query in queries:
        executionTime = fetchFromCache(query)
        print(f"Execution time for query {queryNum} is {executionTime:.4f} seconds")
        queryNum += 1

# Connect to the database
dbConnect = connectToDB()
exec = dbConnect.cursor()

# Measure and print execution times
printExecTime()

# Close the connection
exec.close()
dbConnect.close()
