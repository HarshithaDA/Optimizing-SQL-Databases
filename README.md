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




