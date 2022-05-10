import pyspark
import time

def check_good_contracts(line):
    """
    Function to check if the data is contracts types and contains values for each column
    """
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        float(fields[3]) # Block Number
        return True
    except:
        return False

def check_good_block(line):
    """
    Function to check if the data is Blocks datasets and contains value for each column
    It also sets the below columns as float values
    """
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[0]) # Block Number
        float(fields[6]) # gas_used
        float(fields[7]) # timestamp
        return True
    except:
        return False

# Create Spark Context object
sc = pyspark.SparkContext()

# Print Spark Job Id
print(sc.applicationId)

# Read Contracts Data set from hadoop cluster
lines1 = sc.textFile('/data/ethereum/contracts')
cleaned_contracts = lines1.filter(check_good_contracts)
contracts_block_number = cleaned_contracts.map(lambda l: (l.split(',')[3], 1))

# Read Blocks Data set from hadoop cluster
lines2 = sc.textFile('/data/ethereum/blocks')
cleaned_blocks = lines2.filter(check_good_block)

"""
[0]: block number, [3]: difficulty, [6]: gas_used, [7]: timestamp
"""
mapper_block = cleaned_blocks.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]),int(l.split(',')[6]), time.strftime("%Y-%m", time.gmtime(float(l.split(',')[7]))))))
print(mapper_block.take(2))
# [(u'4776199', (1765656009004680, 2042230, '2017-12')), (u'4776200', (1765656009037448, 4385719, '2017-12'))]

results = mapper_block.join(contracts_block_number)
print(results.take(2))
# [(u'7352442', ((1862016659491710, 6317667, '2019-03'), 1)), (u'7352442', ((1862016659491710, 6317667, '2019-03'), 1))]

updated_result = results.map(lambda x: (x[1][0][2], (x[1][0][0], x[1][0][1], x[1][1])))
print(updated_result.take(2))
# [('2017-08', (1600249491213932, 2032838, 1)), ('2019-06', (2134143517862686, 7989075, 1))]

final = updated_result.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
print(final.take(2))
# [('2019-02', (1084082528199181398629L, 3197663942850, 419164)), ('2016-07', (1662703037969682192, 43543836232, 28773))]

updated_final = final.map(lambda x: (x[0], (float(x[1][0] / x[1][2]), float(x[1][1] / x[1][2])))).sortByKey(ascending=True)
print(updated_final.take(2))
# [('2015-08', (4030960805570, 360218)), ('2015-09', (6577868584193, 540131))]

#Output is of the format Year-Month, Complexity, Avg Gas
updated_final.saveAsTextFile('output_gg_part2')
