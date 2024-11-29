# Import necessary libraries
import mysql.connector
import os
import pandas as pd

# Define shard configurations
shards = {
    "shard_1": {
        "host": "localhost",
        "user": "root",
        "password": "17akrgd18",
        "database": "hospitals_shard_1",
    },
    "shard_2": {
        "host": "localhost",
        "user": "root",
        "password": "17akrgd18",
        "database": "hospitals_shard_2",
    },
    "shard_3": {
        "host": "localhost",
        "user": "root",
        "password": "17akrgd18",
        "database": "hospitals_shard_3",
    },
}

# Create shard connections
def createShardConnections():
    shard_connections = {}
    for shard, config in shards.items():
        shard_connections[shard] = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            allow_local_infile=True,
        )
    cursor = shard_connections[shard].cursor()
    cursor.execute("SET GLOBAL local_infile = 1;")
    cursor.close()
    
    return shard_connections

# Allocate shard based on sharding key
def allocateShard(shard_key):
    shard_key = str(shard_key).strip()

    # Map based on the first digit of the shard_key
    first_digit = shard_key[0]

    if first_digit in ("1", "2", "3"):
        return "shard_7"
    elif first_digit in ("4", "5", "6"):
        return "shard_8"
    elif first_digit in ("7", "8", "9", "0"):
        return "shard_9"
    else:
        raise ValueError(f"Unexpected shard_key: {shard_key}.")
        

# Insert data into the correct shard
def insertIntoShardHosp(batch, table_name, shard_key, shard_connections):
    cursor = shard_connections.cursor()

    temp_file_path = f"C:/Users/keert/OneDrive/Desktop/Masters/Semester 1/Courses/DBMS/Project/Dataset/tmp/hospitals/{shard_key}_hospitals.csv"
    batch.to_csv(temp_file_path, index=False, header=False)
    
    query = f"""
    LOAD DATA LOCAL INFILE '{temp_file_path}'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\\n'
    """
    cursor.execute(query)
    shard_connections.commit()
    cursor.close()
    # Optionally, remove the temporary file after insertion (to clean up)
    os.remove(temp_file_path)

# Insert data into the correct shard for prices table
def insertIntoShardPrice(file_path, table_name, shard_key, shard_connections):
    cursor = shard_connections.cursor()

    temp_file_path = f"C:/Users/keert/OneDrive/Desktop/Masters/Semester 1/Courses/DBMS/Project/Dataset/tmp/prices/{shard_key}_prices.csv"

    batch = pd.read_csv(file_path)
    batch.to_csv(temp_file_path, index=False, header=False)

    query = f"""
    LOAD DATA LOCAL INFILE '{temp_file_path}'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\\n'
    """
    cursor.execute(query)
    shard_connections.commit()
    cursor.close()

    os.remove(temp_file_path)
