# Import the required libraries
import mysql.connector
import pandas as pd
import os

# Connect to SQL Workbench by providing the host/username/password (will be different depending on your steup)
dbConnect = mysql.connector.connect(
    host="YOUR_HOST",  # Provide host from workbench
    user="YOUR_ROOT",       # Provide the name of user you created while setting up workbench        
    password="YOUR_PASSWORD", # Provide the password you created while setting up workbench        
    database="YOUR_DATABASE_NAME",  # Provide the name of the database you created using workbench UI
    allow_local_infile=True  # set local_infile to true to be able to read from file
)

# Path of hospitals table dataset on your system
hospitalsDatasetFilePath = "YOUR_HOSPITALS.CSV_FILEPATH"

# Path of hospitals pries table dataset on your system
hospitalPricesDatasetFilePath = "YOUR_HOSPITALS_PRICES.CSV_FILEPATH"

# Temp file to store 100000 rows from the pries table
filePathLoadPrices = "/tmp/prices.csv"

# Read 100000 rows from prices at a time
limitedRows = 100000

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
PRIMARY KEY (cms_certification_num(6), code(6), inpatient_outpatient, internal_revenue_code(6), code_disambiguator(6), payer(6))
);
"""

# Post creating the table along with the headers, load the data into the hospitals table using LOAD DATA (make sure to set INFILE to true)
loadHospitalsQuery = f"""
            LOAD DATA LOCAL INFILE '{hospitalsDatasetFilePath}'
            INTO TABLE HOSPITALS
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            """

# Post creating the table along with the headers, load the data into the prices table using LOAD DATA (make sure to set INFILE to true)
loadPricesToTempFileQuery = f"""
        LOAD DATA LOCAL INFILE '{filePathLoadPrices}' 
        INTO TABLE PRICES 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        """

# Funtion to create both our tables
def createTables():
    exec.execute(createHospitalsTableQuery)
    exec.execute(createHospitalPricesTableQuery)

# Function to load data into hospitals table
def loadHospitalsData():
    exec.execute(loadHospitalsQuery)

# Function to load data into prices table in batches
def loadHospitalPricesData():
    for batch in pd.read_csv(hospitalPricesDatasetFilePath, chunksize=limitedRows, iterator=True):
        batch.to_csv(filePathLoadPrices, index=False, header=False)
        exec.execute(loadPricesToTempFileQuery)
        os.remove(filePathLoadPrices)

# Fetch cursor to be able to execute queries
exec = dbConnect.cursor()

createTables()
loadHospitalsData()
loadHospitalPricesData()

# Query to verify if the data was loaded properly in hospitals table (Please comment out after initial run)
exec.execute("SELECT COUNT(*) FROM HOSPITALS;")
print("Total rows in hospitals table: ", exec.fetchone()[0])

# Query to verify if the data was loaded properly in prices table (Please comment out after initial run)
exec.execute("SELECT COUNT(*) FROM PRICES;")
print("Total rows in prices table: ", exec.fetchone()[0])
                 
# Close the curesor and the SQL connector
exec.close()
dbConnect.close()

