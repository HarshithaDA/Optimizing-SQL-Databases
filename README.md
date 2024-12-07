# DBProject
Database Design Group Project

Paper Title: Optimizing SQL Databases for Big Data Workloads: Techniques and Best
Practices.

In this project, we've implemented the below optimization techniques mentioned in our paper:
1. Indexing
2. Caching
3. Partitioning
4. Sharding

We have used the following dataset for our implementation - https://www.kaggle.com/datasets/jpmiller/healthcare

Steps to run:

Software needed:
1. Install MySQL
2. Install MySQL Workbench
3. Install Python 
4. Install the mysql-connector and pandas library to connect to the database
5. An IDE to run the script (ex. VS Code)
6. Download the dataset using the mentioned link

MY SQL Workbench setup:
1. Once you install workbench, move it to the applications folder on your system
2. Setup root and your password
3. Run the following command on the UI - CREATE DATABASE "YOUR_DB_NAME";
4. Run the following commands to be able to read the large dataset:
    SET GLOBAL local_infile = 1
    SET GLOBAL wait_timeout = 31536000
    SET GLOBAL interactive_timeout = 31536000
    SET GLOBAL net_read_timeout = 1200
    SET GLOBAL net_write_timeout = 1200
    SET GLOBAL max_allowed_packet = 1073741824

CHANGES to be made in sqlConnector.py
1. Update host, user, password and database in dbConnect - (get these details from workbench)
2. Update the hospitalsDatasetFilePath variable depending on the location of hospitals.csv on your system
3. Update the hospitalPricesDatasetFilePath variable depending on the location of hospital_prices.csv on your system
4. Save the changes and run the scripts - indexing.py, sharding.py, partitioning.py, caching.py

Steps to run Indexing:
1. First run the sqlConnector.py to connect to the database and create and load the tables
2. Then run the indexing.py to run the queries in queries.py and view execution times
   
Steps to run Caching:

1. First run the sqlConnectorCaching.py to implement the connect to the database and create the cache table as well
2. Then run the Caching.py to run the queries in queries.py and view execution times

Steps to run Range Partitioning:

1. First run the sqlConnectorRangePartitioning.py to create the tables and implement the partitions for the same.
2. Next run the rangePartitioning.py file to fetch queries from queries.py and execute them and view each of their execution times.

Steps to run Hash Partitioning:

1. First run the sqlConnectorHashPartitioning.py to create the tables and implement the partitions for the same.
2. Next run the hashPartitioning.py file to fetch queries from queries.py and execute them and view each of their execution times.


Steps to run Sharding:
1. Create databases: 'hospitals_shard_1', 'hospitals_shard_2', 'hospitals_shard_3'.
2. Execute 'sqlConnectorSharding.py' to connect to the databases, create tables, to execute the queries and to calculate the execution time.


Team Members:
Manav Kothari
Keerthi Anand
Harshitha Devina Anto
Veronica Chittora
Siddhi Patil
