import mysql.connector
import os
from sharding import createShardConnections, allocateShard, insertIntoShardHosp, insertIntoShardPrice
from queries import pricesTableName, hospitalsTableName
import pandas as pd

hospitalsDatasetFilePath = "Dataset/hospitals.csv"
hospitalPricesDatasetFilePath = "Dataset/hospital_prices.csv"
filePathLoadPrices = "Dataset/tmp/prices.csv"

# Read 100000 rows from prices at a time
limitedRows = 100000

# Create HOSPITALS Table to store the data in hospitals.csv
createHospitalsTableQuery = f"""
CREATE TABLE IF NOT EXISTS {hospitalsTableName} (
cms_certification_num CHAR(6) NOT NULL,
name VARCHAR(256),
address VARCHAR(256),
city VARCHAR(256),
state  VARCHAR(256),
zip5  CHAR(5),
beds  INTEGER,
phone_number CHAR(10),
homepage_url VARCHAR(256) DEFAULT 'NONE',
chargemaster_url VARCHAR(256) DEFAULT 'NONE',
last_edited_by_username VARCHAR(256) DEFAULT 'NONE',
PRIMARY KEY (cms_certification_num)
);
"""

# Create PRICES Table to store the data in hospitals_prices.csv (Imported from kaggle dataset)
createHospitalPricesTableQuery  = f"""
CREATE TABLE IF NOT EXISTS {pricesTableName} (
cms_certification_num CHAR(6),
payer VARCHAR(256),
code VARCHAR(50) DEFAULT 'NONE',
internal_revenue_code VARCHAR(128) DEFAULT 'NONE',
units VARCHAR(50) DEFAULT 'NONE' ,
description VARCHAR(2048),
inpatient_outpatient ENUM('INPATIENT', 'OUTPATIENT', 'BOTH', 'UNSPECIFIED') DEFAULT 'UNSPECIFIED',
price decimal(10,2) NOT NULL,
code_disambiguator VARCHAR(2048) DEFAULT 'NONE',
CONSTRAINT price_less_than_zero CHECK (price >= 0),
CONSTRAINT price_greater_than_ten_mil CHECK (price <= 1E+7),
CONSTRAINT payer_not_empty CHECK (trim(payer) != ""),
CONSTRAINT code_not_empty CHECK (trim(code) != ""),
CONSTRAINT internal_revenue_code_not_empty CHECK (trim(code) != ""),
CONSTRAINT desc_not_empty CHECK (trim(description) != ""),
CONSTRAINT strings_trimmed CHECK (
payer = trim(payer) AND
code = trim(code) AND
internal_revenue_code = trim(internal_revenue_code) AND
description = trim(description)
),
FOREIGN KEY (cms_certification_num) REFERENCES HOSPITALS(cms_certification_num),
PRIMARY KEY (cms_certification_num(6), code(10), inpatient_outpatient, payer,internal_revenue_code)
);
"""

# Create tables in all shards
def createTables():
    shard_connections = createShardConnections()
    for conn in shard_connections.values():
        cursor = conn.cursor()
        cursor.execute(createHospitalsTableQuery)
        cursor.execute(createHospitalPricesTableQuery)
        conn.commit()
        cursor.close()
    print("Tables created successfully.")

# Load hospitals data into shards
def loadHospitalsData():
    shard_connections = createShardConnections()
    for batch in pd.read_csv(hospitalsDatasetFilePath, chunksize=1000):
        for _, row in batch.iterrows():
            shard_key = row["cms_certification_num"]
            shard = allocateShard(shard_key)
            insertIntoShardHosp(pd.DataFrame([row]), hospitalsTableName, shard_key, shard_connections[shard])

# Load prices data into shards
def loadHospitalPricesData():
    shard_connections = createShardConnections()
    for batch in pd.read_csv(hospitalPricesDatasetFilePath, chunksize=limitedRows, iterator=True):
        batch.to_csv(filePathLoadPrices, index=False, header=False)
        for _, row in batch.iterrows():
            shard_key = row["cms_certification_num"]
            shard = allocateShard(shard_key)
            insertIntoShardPrice(filePathLoadPrices, pricesTableName, shard_key, shard_connections[shard])
    os.remove(filePathLoadPrices)

# Main execution
createTables()

loadHospitalsData()
print("Data loaded successfully into shards for hospitals.")

loadHospitalPricesData()
print("Data loaded successfully into shards for prices.")
