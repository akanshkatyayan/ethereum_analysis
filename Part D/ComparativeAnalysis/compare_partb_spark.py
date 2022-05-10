
import pyspark
from time import *

def check_good_transactions(line):
	""" Function to check if the transactions record is valid"""
	try:
		fields = line.split(",")
		if len(fields) == 7:
			int(fields[3])
			return True
		else:
			return False
	except:
		return False

def check_good_contracts(line):
	""" Function to check if the contracts record is valid"""
	try:
		fields = line.split(",")
		if len(fields) == 5:
			fields[0]
			return True
		else:
			return False
	except:
		return False

# Storing Start Time
start_time = time()

# Spark Context object
sc = pyspark.SparkContext()
print(sc.applicationId)

# Read Transactions Dataset
transaction_data = sc.textFile("/data/ethereum/transactions")

# Validate transactions dataset and take only valid data
valid_transaction_data = transaction_data.filter(check_good_transactions).map(lambda x: x.split(","))

# Map to_address and value
transaction_features = valid_transaction_data.map(lambda x: (x[2], int(x[3])))
result1 = transaction_features.reduceByKey(lambda a,b: a+b)

# Read Contracts Dataset
contracts_data = sc.textFile("/data/ethereum/contracts")
# Validate Contracts datasset and take only valid data
valid_contracts_data = contracts_data.filter(check_good_contracts).map(lambda x: x.split(","))

# Fetch addresses from Contracts as these will be considered as smart contracts
contracts_features = valid_contracts_data.map(lambda x: (x[0], None))

result2 = result1.join(contracts_features)
# (address, (summed_value, None))

top10_data = result2.takeOrdered(10, key = lambda x: -x[1][0])
top10_data = sc.parallelize(top10_data).saveAsTextFile("output_compare_sparkresult2")

# Storing stop time
stop_time = time()
print("Total Time taken by Spark Job: {}".format(stop_time - start_time))
