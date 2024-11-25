# Compare the time of execution of the below queries using different techniques - sharding, partitioning, indexing and caching

pricesTableName = "PRICES"
hospitalsTableName = "HOSPITALS"

queries = ["SELECT cms_certification_num FROM PRICES WHERE description = 'SEIZURES WITH MCC' AND price >= 30593",
"SELECT SUM(price) AS total_price FROM PRICES WHERE payer = 'WELLCARE GA MEDICAID [308019]'",
"SELECT cms_certification_num FROM PRICES WHERE description = 'SEIZURES WITH MCC' AND price >= 30593",
"SELECT cms_certification_num FROM PRICES WHERE description = 'SEIZURES WITH MCC' AND price >= 30593",
"SELECT cms_certification_num FROM PRICES WHERE description = 'SEIZURES WITH MCC' AND price >= 30593"
]