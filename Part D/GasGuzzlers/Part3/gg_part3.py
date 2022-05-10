import pyspark
import time

'''
Output from Part B

"0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444"	84155100809965865822726776
"0xfa52274dd61e1643d2205169732f29114bc240b3"	45787484483189352986478805
"0x7727e5113d1d161373623e5f49fd568b4f543a9e"	45620624001350712557268573
"0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef"	43170356092262468919298969
"0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8"	27068921582019542499882877
"0xbfc39b6f805a9e40e77291aff27aee3c96915bdd"	21104195138093660050000000
"0xe94b04a0fed112f3664e45adb2b8915693dd5ff3"	15562398956802112254719409
"0xbb9bc244d798123fde783fcc1c72d3bb8c189413"	11983608729202893846818681
"0xabbb6bebfa05aa13e908eaa492bd7a8343760477"	11706457177940895521770404
"0x341e790174e3a4d35b65fdc067b6b5634a61caea"	8379000751917755624057500

'''
# Taking random 4 addresses from the above
random_address = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", "0xfa52274dd61e1643d2205169732f29114bc240b3", "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8", "0xbfc39b6f805a9e40e77291aff27aee3c96915bdd"]
global curr_address

def check_good_transaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
            
        if fields[2] == curr_address:
            return True
    except:
        return False

def check_good_blocks(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        float(fields[0]) # block number
        float(fields[6]) # gas used
        float(fields[7]) # timestamp
        return True
    except:
        return False


for current_address in random_address:
    # Create Spark context object
    curr_address = current_address
    sc = pyspark.SparkContext()
    print(sc.applicationId)
    
    # Reading Transactions Dataset
    transactions_data = sc.textFile('/data/ethereum/transactions')
    valid_transactions_data = transactions_data.filter(check_good_transaction)
    transaction_block_number = valid_transactions_data.map(lambda l: (l.split(',')[0], l.split(',')[2]))
    # print(transaction_block_number.take(2))
    # [(u'2280145', u'0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444'), (u'2280145', u'0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444')]

    blocks_data = sc.textFile('/data/ethereum/blocks')
    valid_blocks_data = blocks_data.filter(check_good_blocks)

    blocks_mapper = valid_blocks_data.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]), time.strftime("%Y-%m", time.gmtime(float(l.split(',')[7]))))))
    print(blocks_mapper.take(2))
    # [(u'4776199', (1765656009004680, '2017-12')), (u'4776200', (1765656009037448, '2017-12'))]

    results1 = blocks_mapper.join(transaction_block_number)
    # print(results.take(2))
    # [(u'5003843', ((2604480603800323, '2018-01'), u'0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444')), (u'2114577', ((64061501150351, '2016-08'), u'0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444'))]

    step1 = results1.map(lambda x: (x[1][0][1], x[1][0][0]))
    # print(step1.take(2))
    # [('2017-09', 2925691023921253), ('2017-12', 1574664440410841)]

    step2 = step1.reduceByKey(lambda a,b: float(a+b)).sortByKey(ascending=True)
    # print(step2.take(2))
    # [('2016-07', 2.063280508261113e+16), ('2016-08', 1.9618904264539485e+17)]

    step2.saveAsTextFile('Result_address_'+ current_address)

    sc.stop()


