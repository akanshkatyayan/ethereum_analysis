from pyspark.sql.functions import *
import pyspark
import json

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions
            str(fields[2]) # to_address
            if int(fields[3]) == 0: # skip data with value = 0
                return False
            return True
    except:
        return False

sc = pyspark.SparkContext()
# print Spark Objecct Application ID
print(sc.applicationId)

# Context for CSV Data
newcontext = pyspark.SQLContext(sc)

# Read CSV using SQL Context object
df_scams = newcontext.read.option('header', False).format('csv').load("scams.csv")

# Convert SQL Context object into RDD
scams_rdd = df_scams.rdd.map(lambda x: (x[0],(x[1],x[2])))

# Read Transactions dataset 
transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)

"""fetch required fields from transactions dataset
[2]: to_address
[3]: values
[4]: Gas
"""
step1 = transactions.map(lambda l:  (l.split(',')[2], (float(l.split(',')[4]), float(l.split(',')[3]))))

# Join Transactions dataset with Scams records
step2 = step1.join(scams_rdd)

# Sum values and gas values based on Scam Type
step_add = step2.map(lambda x: (x[1][1][0], (x[1][0][0], x[1][0][1])))

# Reduce by Scam Type as a key 
sum_reduce= step_add.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
sum_reduce.saveAsTextFile('output_scams_analysis_part1')

# Part 2
# Key as Scam Type + Status , Value as Gas
new_map = step2.map(lambda x: (x[1][1],  x[1][0][0]))

# Reduce by key - Scam Type + Status
new_value = new_map.reduceByKey(lambda a,b: a+b).sortByKey(ascending=True)
new_value.saveAsTextFile('output_scams_analysis_part2')
