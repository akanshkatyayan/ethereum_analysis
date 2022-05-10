import pyspark
import time
import pyspark.sql.functions as F


def check_good_lines(line):
    """
    Function to validate Transactions Dataset
    """
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False

        float(fields[3])
        return True
    except:
        return False

# Create Spark Context Object
sc = pyspark.SparkContext()
print(sc.applicationId)

#reading the etherum data from the HDFS
transactions_data = sc.textFile("/data/ethereum/transactions")

#fvalidating the transactions dataset
valid_transactions_data = transactions_data.filter(check_good_lines)

# Scenario: Self Trade

# RDD with required values
requiredtransactions = valid_transactions_data.map(lambda x: (x.split(',')[1], x.split(',')[2],  x.split(',')[3],  x.split(',')[6]))

columns = ['from_address', 'to_address', 'value',' timestamp']
# Create Dtaaframe from RDD and give column names using columns list
df = requiredtransactions.toDF(columns)

# Using sql functions to get rows with same column values.
df1 = df.filter(F.col('from_address') == F.col('to_address'))

# Create RDD with Key as from and to address and value as value
self_transaction_rdd = df1.rdd.map(lambda x: ((x[0],x[1]), float(x[2])))
# print(self_transaction_rdd.take(3))
# [((u'0x7ed1e469fcb3ee19c0366d829e291451be638e59', u'0x7ed1e469fcb3ee19c0366d829e291451be638e59'), (u'9650000000000000', u'1474189901')), ((u'0xceac8d547a1cf569a6b93cacf0e19094030e8776', u'0xceac8d547a1cf569a6b93cacf0e19094030e8776'), (u'2259220000000000', u'1474198827'))]

# RDD to sum values for same from and to address
result = self_transaction_rdd.reduceByKey(lambda a, b: a+b)

# fetch top 10 trades by value
top10_data = result.takeOrdered(10, key = lambda x: -x[1])
top10_data = sc.parallelize(top10_data).saveAsTextFile("wash_trades_top10")

