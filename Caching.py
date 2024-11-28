import time
import json
from sqlConnector import connectToDB
from queries import queries

def get_query(query):
    
    # We must first check if the query exists in the cache, get time to retrieve from cache
    start_cache_time = time.time()
    exec.execute("SELECT result, execution_time FROM query_cache WHERE query = %s", (query,))
    cached_entry = exec.fetchone()
    execution_from_cache = time.time() - start_cache_time

    if cached_entry:
        # Cache hit - Set the result and execution time to the calculated values
        print(f"Query in cache: {query}")
        result = json.loads(cached_entry[0])
        execution_time = execution_from_cache
    else:
        # Cache miss - Execute the query on the database
        print(f"Query not in cache: {query}")
        start_time = time.time()
        exec.execute(query) 
        result = exec.fetchall()
        execution_time = time.time() - start_time

        # Store the query, result, and execution time in the cache for future use
        exec.execute(
            """
            INSERT INTO query_cache (query, result, execution_time) 
            VALUES (%s, %s, %s)
            """,
            (query, json.dumps(result), execution_time)  # Convert result to JSON
        )
        dbConnect.commit()

    return result, execution_time

def print_exec_time():
    # Prints execution time for each query in the list.
    queryNum = 1
    for query in queries:
        result, execution_time = get_query(query)
        print(f"Execution time for query {queryNum} is {execution_time:.4f} seconds")
        queryNum += 1

# Connect to the database
dbConnect = connectToDB()
exec = dbConnect.cursor()

# Measure and print the execution times
print_exec_time()

# Close the connection
exec.close()
dbConnect.close()
