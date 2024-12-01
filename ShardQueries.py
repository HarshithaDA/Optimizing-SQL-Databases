import mysql.connector
from Sharding import createShardConnections, allocateShard
from queries import hospitalsTableName, pricesTableName
import time

# Fetch hospital details for a given cms_certification_num from the appropriate shard.
def getHospitalDetails(cms_certification_num):
    shard_key = cms_certification_num
    shard = allocateShard(shard_key)
    connection = createShardConnections()[shard]
    
    query = f"""
    SELECT * 
    FROM {hospitalsTableName} 
    WHERE cms_certification_num = '{cms_certification_num}';
    """
    
    start_time = time.time()  # Start timing
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()  # Fetch results from the shard
    end_time = time.time()  # End timing
    
    cursor.close()
    connection.close()
    print(f"Query Execution Time (getHospitalDetails): {end_time - start_time:.6f} seconds")
    return result

# Calculate the total number of beds across all shards.
def getTotalBeds():
    
    shard_connections = createShardConnections() 
    total_beds = 0
    
    start_time = time.time()  # Start timing
    for shard, connection in shard_connections.items():
        query = f"""
        SELECT SUM(beds) AS total_beds 
        FROM {hospitalsTableName};
        """
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchone()  # Fetch result from each shard
        total_beds += result["total_beds"] if result["total_beds"] else 0
        cursor.close()
        connection.close()
    end_time = time.time()  # End timing
    
    print(f"Query Execution Time (getTotalBeds): {end_time - start_time:.6f} seconds")
    return total_beds

#Fetch price details for a specific hospital from the appropriate shard.
def getHospitalPrices(cms_certification_num):

    shard_key = cms_certification_num
    shard = allocateShard(shard_key) 
    connection = createShardConnections()[shard] 
    
    query = f"""
    SELECT h.name, h.city, h.state, p.description, p.price
    FROM {hospitalsTableName} h
    JOIN {pricesTableName} p
    ON h.cms_certification_num = p.cms_certification_num
    WHERE h.cms_certification_num = '{cms_certification_num}';
    """
    
    start_time = time.time()  # Start timing
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()  # Fetch results
    end_time = time.time()  # End timing
    
    cursor.close()
    connection.close()
    print(f"Query Execution Time (getHospitalPrices): {end_time - start_time:.6f} seconds")
    return result
