# %load "C:\Users\Harshitha\Downloads\sqlConnectorHashPartitioning.py"
# Import the required libraries
import mysql.connector
import pandas as pd
import os

# Connect to SQL Workbench by providing the host/username/password (will be different depending on your steup)
dbConnect = mysql.connector.connect(
    host="127.0.0.1",  # Provide host from workbench
    user="root",       # Provide the name of user you created while setting up workbench        
    password="Harshidbmsproject24@", # Provide the password you created while setting up workbench        
    database="mydatabase5",  # Provide the name of the database you created using workbench UI
    allow_local_infile=True  # set local_infile to true to be able to read from file
)

# Fetch cursor to be able to execute queries
exec = dbConnect.cursor()

# Path of hospitals table dataset on your system
hospitalsDatasetFilePath = "C:/Users/Harshitha/Downloads/hospitals.csv"

# Path of hospitals pries table dataset on your system
hospitalPricesDatasetFilePath = "D:/hospital_prices.csv"

# Temp file to store 50000 rows from the pries table
filePathLoadPrices = "C:/Users/Harshitha/AppData/Local/Temp/prices.csv"

# Read 50000 rows from prices at a time
limitedRows = 50000

# Create HOSPITALS Table to store the data in hospitals.csv
createHospitalsTableQuery = """
CREATE TABLE IF NOT EXISTS HOSPITALS (
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
last_edited VARCHAR(256) DEFAULT 'NONE',
PRIMARY KEY (cms_certification_num)
);
"""

# Create PRICES Table to store the data in hospitals_prices.csv (Imported from kaggle dataset)
createHospitalPricesTableQuery  = """
CREATE TABLE IF NOT EXISTS PRICES (
cms_certification_num CHAR(6),
payer VARCHAR(256),
code VARCHAR(128) DEFAULT 'NONE',
internal_revenue_code VARCHAR(128) DEFAULT 'NONE',
units VARCHAR(128) DEFAULT 'NONE' ,
description VARCHAR(2048),
inpatient_outpatient ENUM('INPATIENT', 'OUTPATIENT', 'BOTH', 'UNSPECIFIED') DEFAULT 'UNSPECIFIED',
price decimal(10,2) NOT NULL,
price_partition INT GENERATED ALWAYS AS (price * 100) STORED, -- Add partition column
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
PRIMARY KEY (cms_certification_num(6), code(6), inpatient_outpatient, internal_revenue_code(6), code_disambiguator(6), payer(6), price_partition)
)
PARTITION BY HASH(MOD(price_partition, 100))
PARTITIONS 4;  -- Number of partitions

"""

# Post creating the table along with the headers, load the data into the hospitals table using LOAD DATA (make sure to set INFILE to true)
def loadHospitalsData():
    loadHospitalsQuery = f"""
        LOAD DATA LOCAL INFILE '{hospitalsDatasetFilePath}'
        INTO TABLE HOSPITALS
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n';
    """
    exec.execute(loadHospitalsQuery)
    dbConnect.commit()

# Post creating the table along with the headers, load the data into the prices table using LOAD DATA (make sure to set INFILE to true)
# Validate cms_certification_num and load prices data
def loadHospitalPricesData():
    invalidRows = []  # List to store invalid rows

    for batch in pd.read_csv(hospitalPricesDatasetFilePath, chunksize=limitedRows, iterator=True, low_memory=False):
        # Validate cms_certification_num exists in HOSPITALS
        validBatch = []
        for _, row in batch.iterrows():
            cms_certification_num = row['cms_certification_num']
            exec.execute(
                "SELECT COUNT(*) FROM HOSPITALS WHERE cms_certification_num = %s;",
                (cms_certification_num,)
            )
            if exec.fetchone()[0] > 0:
                validBatch.append(row)
            else:
                invalidRows.append(row)

        # Save valid rows to a temp file and load them
        validBatchDF = pd.DataFrame(validBatch)
        validBatchDF.to_csv(filePathLoadPrices, index=False, header=False)
        loadPricesToTempFileQuery = f"""
            LOAD DATA LOCAL INFILE '{filePathLoadPrices}' 
            INTO TABLE PRICES 
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n';
        """
        exec.execute(loadPricesToTempFileQuery)
        dbConnect.commit()
        os.remove(filePathLoadPrices)

    # Log invalid rows
    if invalidRows:
        invalidRowsDF = pd.DataFrame(invalidRows)
        invalidRowsDF.to_csv("invalid_prices.csv", index=False)
        print(f"Invalid rows logged in 'invalid_prices.csv'.")


# Create tables and load data
exec.execute(createHospitalsTableQuery)
exec.execute(createHospitalPricesTableQuery)
loadHospitalsData()
loadHospitalPricesData()

# Query to verify if the data was loaded properly in hospitals table (Please comment out after initial run)
exec.execute("SELECT COUNT(*) FROM HOSPITALS;")
print("Total rows in hospitals table: ", exec.fetchone()[0])

# Query to verify if the data was loaded properly in prices table (Please comment out after initial run)
exec.execute("SELECT COUNT(*) FROM PRICES;")
print("Total rows in prices table: ", exec.fetchone()[0])
                 
# Close the cursor and the SQL connector
exec.close()
dbConnect.close()

