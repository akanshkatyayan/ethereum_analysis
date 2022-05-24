# **Ethereum Analysis**
Ethereum Data Analysis on contracts, transactions, gas, scams and scammers using Big Data technologies - MapReduce and Spark

Ethereum is a blockchain-based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mine their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralized, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

* [Total number of transactions](https://github.com/akanshkatyayan/ethereum_analysis/#part-a.-time-analysis-(20%))

* [Top 10 most popular services](https://github.com/Dorsa-Arezooji/Etherium-Analysis#2-top-10-most-popular-services)

* [Most lucrative forms of scam](https://github.com/Dorsa-Arezooji/Etherium-Analysis#31-most-lucritive-forms-of-scams)

* [How different scams changed with time](https://github.com/Dorsa-Arezooji/Etherium-Analysis#32-how-different-scams-changed-with-time)

* [Transactions gas price vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis#41-transactions-gas-price-vs-time)

* [Contract gas vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis#42-contract-gas-vs-time)

* [Contract complexity vs time](https://github.com/Dorsa-Arezooji/Etherium-Analysis#43-contract-complexity-vs-time)

* [Triangle count](https://github.com/Dorsa-Arezooji/Etherium-Analysis#51-triangle-count)

* [Scammer wallets used for accumulating Ether](https://github.com/Dorsa-Arezooji/Etherium-Analysis#52-scammer-wallets)


## **Dataset**
A subset of the data available on BigQuery is provided at the HDFS folder /data/ethereum. The blocks, contracts, and transactions tables have been pulled down and been stripped of unneeded fields to reduce their size. Please find them attached as csv files. We have also downloaded a set of scams, both active and inactive, run on the Ethereum network via etherscamDB which is available on HDFS at /data/ethereum/scams.json.


### **DATASET SCHEMA - BLOCKS**
number: The block number
hash: Hash of the block
miner: The address of the beneficiary to whom the mining rewards were given
difficulty: Integer of the difficulty for this block
size: The size of this block in bytes
gas_limit: The maximum gas allowed in this block
gas_used: The total used gas by all transactions in this block
timestamp: The timestamp for when the block was collated
transaction_count: The number of transactions in the block
```
+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+-----------------+
| number|                hash|               miner|      difficulty| size|gas_limit|gas_used| timestamp|transaction_count|
+-------+--------------------+--------------------+----------------+-----+---------+--------+----------+-----------------+
|4776199|0x9172600443ac88e...|0x5a0b54d5dc17e0a...|1765656009004680| 9773|  7995996| 2042230|1513937536|               62|
|4776200|0x1fb1d4a2f5d2a61...|0xea674fdde714fd9...|1765656009037448|15532|  8000029| 4385719|1513937547|              101|
|4776201|0xe633b6dca01d085...|0x829bd824b016326...|1765656009070216|14033|  8000000| 7992282|1513937564|               99|
```
### **DATASET SCHEMA - TRANSACTIONS**

block_number: Block number where this transaction was in
from_address: Address of the sender
to_address: Address of the receiver. null when it is a contract creation transaction
value: Value transferred in Wei (the smallest denomination of ether)
gas: Gas provided by the sender
gas_price : Gas price provided by the sender in Wei
block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)
```
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|block_number|        from_address|          to_address|              value|   gas|  gas_price|block_timestamp|
+------------+--------------------+--------------------+-------------------+------+-----------+---------------+
|     6638809|0x0b6081d38878616...|0x412270b1f0f3884...| 240648550000000000| 21000| 5000000000|     1541290680|
|     6638809|0xb43febf2e6c49f3...|0x9eec65e5b998db6...|                  0| 60000| 5000000000|     1541290680|
|     6638809|0x564860b05cab055...|0x73850f079ceaba2...|                  0|200200| 5000000000|     1541290680|
```

### **DATASET SCHEMA - CONTRACTS**

address: Address of the contract
is_erc20: Whether this contract is an ERC20 contract
is_erc721: Whether this contract is an ERC721 contract
block_number: Block number where this contract was created
```
+--------------------+--------+---------+------------+--------------------+
|             address|is_erc20|is_erc721|block_number|     block_timestamp|
+--------------------+--------+---------+------------+--------------------+
|0x9a78bba29a2633b...|   false|    false|     8623545|2019-09-26 08:50:...|
|0x85aa7fbc06e3f95...|   false|    false|     8621323|2019-09-26 00:29:...|
|0xc3649f1e59705f2...|   false|    false|     8621325|2019-09-26 00:29:...|
```

### **DATASET SCHEMA - SCAMS.JSON**
id: Unique ID for the reported scam
name: Name of the Scam
url: Hosting URL
coin: Currency the scam is attempting to gain
category: Category of scam - Phishing, Ransomware, Trust Trade, etc.
subcategory: Subdivisions of Category
description: Description of the scam provided by the reporter and datasource
addresses: List of known addresses associated with the scam
reporter: User/company who reported the scam first
ip: IP address of the reporter
status: If the scam is currently active, inactive or has been taken offline

```
0x11c058c3efbf53939fb6872b09a2b5cf2410a1e2c3f3c867664e43a626d878c0: {
    id: 81,
    name: "myetherwallet.us",
    url: "http://myetherwallet.us",
    coin: "ETH",
    category: "Phishing",
    subcategory: "MyEtherWallet",
    description: "did not 404.,MEW Deployed",
    addresses: [
        "0x11c058c3efbf53939fb6872b09a2b5cf2410a1e2c3f3c867664e43a626d878c0",
        "0x2dfe2e0522cc1f050edcc7a05213bb55bbb36884ec9468fc39eccc013c65b5e4",
        "0x1c6e3348a7ea72ffe6a384e51bd1f36ac1bcb4264f461889a318a3bb2251bf19"
    ],
    reporter: "MyCrypto",
    ip: "198.54.117.200",
    nameservers: [
        "dns102.registrar-servers.com",
        "dns101.registrar-servers.com"
    ],
    status: "Offline"
},
```

## PART A. TIME ANALYSIS (20%)
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset. Create a bar plot showing the average value of transactions in each month between the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.
Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)

### _Results:_
**The total number of transactions are aggregated for each month included in the dataset.**

<img width="595" alt="image" src="https://user-images.githubusercontent.com/35501313/170018630-13b12750-5a21-4ba2-a27a-64f4d9e0cb14.png">


**The average value of transactions in each month between the start and end of the dataset.**

<img width="583" alt="image" src="https://user-images.githubusercontent.com/35501313/170018816-f7d626fa-affb-4485-b499-4865b46c6d32.png">

## **PART B. TOP TEN MOST POPULAR SERVICES (25%)**
Evaluate the top 10 smart contracts by total Ether received. An outline of the subtasks required to extract this information is provided below, focusing on a MRJob based approach. This is, however, is not the only way to complete the task, as there are several other viable ways of completing this assignment.

_JOB 1 - INITIAL AGGREGATION_
To workout which services are the most popular, you will first have to aggregate transactions to see how much each address within the user space has been involved in. You will want to aggregate value for addresses in the to_address field. This, in essense, will be similar to the wordcount problem that we saw in Lab 1 and Lab 2.

_JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING_
Once you have obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contracts (example here and slides here). You will want to join the to_address field from the output of Job 1 with the address field of contracts .

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.

_JOB 3 - TOP TEN_
Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer, utilizing what you have learned from lab 4.

### _Results:_
the top 10 services with the highest amounts of Ethereum received for smart contracts are yielded.

rank | address | total Ether received
-----|---------|------------
1 | 0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444 | 8.415510081e+25
2 | 0xfa52274dd61e1643d2205169732f29114bc240b3 | 4.57874844832e+25
3 | 0x7727e5113d1d161373623e5f49fd568b4f543a9e | 4.56206240013e+25
4 | 0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef | 4.31703560923e+25
5 | 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8 | 2.7068921582e+25
6 | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd | 2.11041951381e+25
7 | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3 | 1.55623989568e+25
8 | 0xbb9bc244d798123fde783fcc1c72d3bb8c189413 | 1.19836087292e+25
9 | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477 | 1.17064571779e+25
10 | 0x341e790174e3a4d35b65fdc067b6b5634a61caea | 8.37900075192e+24


## **PART C. TOP TEN MOST ACTIVE MINERS (15%)**
Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.

### _Results:_

**The top 10 most active miners:**
Rank | Miner | Total size
------------|---------------------------------|---------
1 | "0xea674fdde714fd979de3edf0f56aa9716b898ec8" | 23989401188
2 | "0x829bd824b016326a401d083b33d092293333a830" | 15010222714
3 | "0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c" | 13978859941
4 | "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5" | 10998145387
5 | "0xb2930b35844a230f00e51431acae96fe543a0347" | 7842595276
6 | "0x2a65aca4d5fc5b5c859090a6c34d164135398226" | 3628875680
7 | "0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01" | 1221833144
8 | "0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb" | 1152472379
9 | "0x1e9939daaad6924ad004c2560e90804164900341" | 1080301927
10 | "0x61c808d82a3ac53231750dadc13c777b59310bd9" | 692942577


## **PART D. DATA EXPLORATION (40+%)**

### _Popular Scams:_ 
Utilising the provided scam dataset, what is the most lucrative form of scam? Does this correlate with certainly known scams going offline/inactive? For the correlation, you could produce the count of how many scams for each category are active/inactive/offline/online/etc and try to correlate it with volume (value) to make conclusions on whether state plays a factor in making some scams more lucrative. Therefore, getting the volume and state of each scam, you can make a conclusion whether the most lucrative ones are ones that are online or offline or active or inactive. So for that purpose, you need to just produce a table with SCAM TYPE, STATE, VOLUME which would be enough (15%).

### _Results:_

_Part 1:_ **Scamming is the most lucrative type of scam**
rank | most lucrative scam (category) | Total Gas Used | Total Ether profited
-----|--------------------------------|------------|------------
1 | Scamming | 724922480.0 | 4.372701002504075e+22
2 | Phishing | 5978937963.0 | 4.47158060984405e+22
3 | Fake ICO | 7402302.0 | 1.35645756688963e+21



_Part 2:_ **The output of the correlation implies that the Gas value is more when the scam is active or in an offline status. Gas value reduces when the scam is Inactive and is the minimum when the scam is Suspended.**

Scam (category) | Status | Total Gas Used | Total Value
--------------------------------|------------|------------|------------
Fake ICO | Offline | 7402302.0 | 1.35645756688963e+21
Phishing | Active | 111750772.0 | 6.256455846464806e+21
Phishing | Inactive | 2255249.0 | 1.488677770799503e+19
Phishing | Offline | 610020459.0 | 3.745402749273795e+22
Phishing | Suspended | 896000.0 | 1.63990813e+18
Scamming | Active | 4282186989.0 | 2.2612205279194257e+22
Scamming | Offline | 1694248234.0 | 2.2099890651296327e+22
Scamming | Suspended | 2502740.0 | 3.71016795e+18


### _Gas Guzzlers_ 
For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? Also, could you correlate the complexity for some of the top-10 contracts found in Part-B by observing the change over their transactions (10%)

### _Results:_
_Part 1:_ **Gas Price change with time**

<img width="476" alt="image" src="https://user-images.githubusercontent.com/35501313/170025871-e5abd222-6b55-4785-8d4e-b303c35c2b7d.png">

<img width="497" alt="image" src="https://user-images.githubusercontent.com/35501313/170025978-61ce5559-e35f-421d-a9ec-1a0d8fb30f9c.png">

* Above Bar plot and Line plots show that the gas-price change is at the peak when started in Aug 2018 and then decreased. However, some sudden rise (peaks) can be observed in the gas price during Nov to Feb months of every year.


_Part 2:_ **Have contracts become more complicated, requiring more gas, or less so?**

<img width="601" alt="image" src="https://user-images.githubusercontent.com/35501313/170026596-28efb711-9d5a-439f-9722-1e3f8dcf859a.png">

* Above graphs implies that, the average gas usage increased steadily until the end of 2017 and reached the peak value. From the start of 2018, there were minor changes observed in the similar range.
* On the other hand, complexity (difficulty value) increased exponentially till the Sep 2017 and suddenly observed a decline in the later 2017 and It started to rise again and reached the peak value in Oct 2018. Post which it gradually declined again and couldn’t reach the optimum value.

_Part 3:_ ** could you correlate the complexity for some of the top-10 contracts found in Part-B by observing the change over their transactions?**

**For Address: “0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444” →**

<img width="605" alt="image" src="https://user-images.githubusercontent.com/35501313/170027022-e5e923a4-0160-4cc4-aef7-ae547ade9b5b.png">

**For address: “0xfa52274dd61e1643d2205169732f29114bc240b3”→**

<img width="603" alt="image" src="https://user-images.githubusercontent.com/35501313/170027122-f9e2ff65-9721-492c-a077-6ebc09f544d3.png">

**From the above observations, it can be concluded that the difficulty is correlated with time, and it increases first reaches the peak value in later 2017 and early 2018 and then decreases.**

### _Comparative Evaluation_ 
Reimplement Part B in Spark (if your original was MRJob, or vice versa). How does it run in comparison? Keep in mind that to get representative results you will have to run the job multiple times, and report median/average results. Can you explain the reason for these results? What framework seems more appropriate for this task? (10%)

### _Results:_

* Time taken by Spark and Map Reduce to finish the same task:

<img width="651" alt="image" src="https://user-images.githubusercontent.com/35501313/170027488-7826c38c-e376-43b7-a8d0-979119ae1c63.png">

**For the given dataset, Spark is well suited as compared to MapReduce.**


### _Wash Trading:_ 
Wash trading is defined as "Entering into or purporting to enter into, transactions to give the appearance that purchases and sales have been made, without incurring market risk or changing the trader’s market position" Unregulated exchanges use these to fake up to 70% of their trading volume? Which addresses are involved in wash trading? Which trader has the highest volume of wash trades? How certain are you of your result? More information can be found at https://dl.acm.org/doi/pdf/10.1145/3442381.3449824. One way to measure ether balance over time is also possible but you will need to discuss accuracy concerns. (20%)

### _Results:_

Self trade is the most common type of wash trade. Top 10 self-trades are as below:

From Address | To Address | Total Value
--------------------------------|--------------------------------|--------------
0x02459d2ea9a008342d8685dae79d213f14a87d43 | 0x02459d2ea9a008342d8685dae79d213f14a87d43 |  1.9548531332493176e+25
0x32362fbfff69b9d31f3aae04faa56f0edee94b1d | 0x32362fbfff69b9d31f3aae04faa56f0edee94b1d |  5.295490520134211e+24
0x0c5437b0b6906321cca17af681d59baf60afe7d6 | 0x0c5437b0b6906321cca17af681d59baf60afe7d6 |  2.3771525723546667e+24
0xdb6fd484cfa46eeeb73c71edee823e4812f9e2e1 | 0xdb6fd484cfa46eeeb73c71edee823e4812f9e2e1 |  4.1549736829070815e+23
0xd24400ae8bfebb18ca49be86258a3c749cf46853 | 0xd24400ae8bfebb18ca49be86258a3c749cf46853 |  2.2700012958e+23
0xa9c7d31bb1879bff8be25ead2f59b310a52b7c5a | 0xa9c7d31bb1879bff8be25ead2f59b310a52b7c5a |  1.6966220991798057e+23
0x6cc5f688a315f3dc28a7781717a9a798a59fda7b | 0x6cc5f688a315f3dc28a7781717a9a798a59fda7b |  1.575893013642e+23
0x5b76fbe76325b970dbfac763d5224ef999af9e86 | 0x5b76fbe76325b970dbfac763d5224ef999af9e86 |  7.873327492788825e+22
0xdd3e4522bdd3ec68bc5ff272bf2c64b9957d9563 | 0xdd3e4522bdd3ec68bc5ff272bf2c64b9957d9563 |  5.790175685075673e+22
0x005864ea59b094db9ed88c05ffba3d3a3410592b | 0x005864ea59b094db9ed88c05ffba3d3a3410592b |  3.7199e+22

**Highest volume self-trade wash-trader is with address - 0x02459d2ea9a008342d8685dae79d213f14a87d43**


### _Fork the Chain:_ 
There have been several forks of Ethereum in the past. Identify one or more of these and see what effect it had on price and general usage. For example, did a price surge/plummet occur, and who profited most from this? (10%)

### _Results:_

* Constantinople Fork – Date: 28/2/2019

<img width="519" alt="image" src="https://user-images.githubusercontent.com/35501313/170027728-9116b7eb-f8bb-489a-8ff4-a8d1ca7223d3.png">

* Byzantium Fork – Date: 16/10/2017

<img width="518" alt="image" src="https://user-images.githubusercontent.com/35501313/170027842-3cb76209-e174-4769-8747-b2e74230a01e.png">

* Spurious Dragon – Date: 23/11/2016

<img width="530" alt="image" src="https://user-images.githubusercontent.com/35501313/170027966-e66bcdf8-429a-4d2a-8a94-4a81e9153eef.png">

* Homestead – Date: 15/3/2016

<img width="523" alt="image" src="https://user-images.githubusercontent.com/35501313/170028044-77b3ebd3-ec07-4db7-b831-701204ec0485.png">

In the first example, Constantinople which occurred on 28/2/2019, it can be observed that the avg gas price increased and reached the peak value in 14 days and there is a steep downfall in the average gas price after 28/2/2019.

In the case of Byzantium which occurred on 16/10/2017, it can be observed that there is a continuous decline in the average gas price after the fork for next 3 days.

In case of Spurious Dragon which occurred on 23/11/2016, the gas price decreased drastically and started recovering again after 2 days of the fork.

Finally, In case of Homestead which occurred on 15/3/2016, the gas price slightly decreased but fluctuated for next few days.

Thus, we can conclude that, the forks had a plummet impact on the gas price of the transactions. This means that the top 10 transactions will gain the most profit as the gas price goes down for the transactions, so each transaction will provide the profit in terms of cost.

