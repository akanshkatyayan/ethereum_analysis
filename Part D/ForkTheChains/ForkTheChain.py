import pyspark
import datetime
import time

# Sample Forks Identified in the given timeframe
fork_date_and_name = [[datetime.datetime(2016,3,15),'Homestead'], [datetime.datetime(2016,11,23),'Spurious Dragon'],  [datetime.datetime(2017,10,16),'Byzantium'],  [datetime.datetime(2019,2,28), 'Constantinople']]

def check_good_line(line):
    """
    Function to get valid records and check if the record timstamp is within the range of 7 days prior and after
    """
    try:
        fields = line.split(',')

        block_time = datetime.datetime.fromtimestamp(int(fields[6]))
        block_date = time.gmtime(float(fields[6]))
        address = fields[2]
        gas_value = int(fields[5])

        fork_record = False

        for fork_data in fork_date_and_name:
            start_date = fork_data[0] + datetime.timedelta(-7) # Start Date 7 days prior the fork date
            end_date = fork_data[0] + datetime.timedelta(7) # End Days 7 days after the fork date

            if (block_time > start_date) and (block_time < end_date):
                fork_name = fork_data[1] # Fetch Current fork name
                fork_record = True
                break
        if fork_record:
            return True

    except:
        return False


def mapper(line):
    # mapper function to apply on data to get the required timestamp data only
    try:
        fields = line.split(',')

        block_time = datetime.datetime.fromtimestamp(int(fields[6]))
        block_date = time.gmtime(float(fields[6]))
        address = fields[2]
        gas_value = int(fields[5])

        fork_record = False

        for fork_data in fork_date_and_name:
            start_date = fork_data[0] + datetime.timedelta(-7) # Start Date 7 days prior the fork date
            end_date = fork_data[0] + datetime.timedelta(7) # End Days 7 days after the fork date

            if (block_time > start_date) and (block_time < end_date):
                fork_name = fork_data[1] # Fetch Current fork name
                fork_record = True
                break
        if fork_record:
            full_date = str(block_date.tm_year)+'-'+str(block_date.tm_mon)+'-'+str(block_date.tm_mday)
            return ((full_date, address), (gas_value, fork_name, 1))
    except:
        pass


# Create Spark Context Object
sc = pyspark.SparkContext()
print(sc.applicationId)

# Read Transactions dataset
transactions_data = sc.textFile('/data/ethereum/transactions')
valid_transactions_data = transactions_data.filter(check_good_line)

# Apply mapper function to get data in the range of forks
step1 = valid_transactions_data.map(mapper)
print(step1.take(2))

# Map ton key => fork name and date and values as gas value and counter
step2 = step1.map(lambda x: ((x[1][1], x[0][0]), (x[1][0], x[1][2])))
print(step2.take(2))

# Add Gas values and counter by reducing on key 
step3 = step2.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

# Map on fork name as key and average gas value as value for each date
step4 = step3.map(lambda x: (x[0], float(x[1][0] / x[1][1]))).sortByKey(ascending = True)

# Save output
step4.saveAsTextFile('output_forkthechain')
