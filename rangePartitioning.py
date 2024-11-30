import mysql.connector
import time
from queries import queries 

def connectToDB():
    """Establish a connection to the database."""
    dbConnect = mysql.connector.connect(
        host="127.0.0.1",  # Provide host
        user="root",       # Provide username
        password="Harshidbmsproject24@",  # Provide password
        database="mydatabase3",  # Database name
        allow_local_infile=True
    )
    return dbConnect

def measureQueryExecutionTime(cursor, query):
    """
    Measure the execution time of a query.
    Args:
        cursor: The MySQL cursor object.
        query: The SQL query to execute.
    Returns:
        float: Execution time in seconds.
    """
    start_time = time.time()  # Record start time
    cursor.execute(query)  # Execute the query
    cursor.fetchall()  # Fetch all results to ensure query completes
    end_time = time.time()  # Record end time
    return end_time - start_time

def main():
    """Main function to execute and time queries."""
    dbConnect = connectToDB()
    cursor = dbConnect.cursor()

    print("Testing query execution times...\n")
    for i, query in enumerate(queries, start=1):
        print(f"Executing Query {i}: {query}")
        execution_time = measureQueryExecutionTime(cursor, query)
        print(f"Query {i} executed in {execution_time:.6f} seconds.\n")

    # Close resources
    cursor.close()
    dbConnect.close()

if __name__ == "__main__":
    main()
