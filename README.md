# ethereum_analysis
Ethereum Data Analysis using Big Data technologies - MapReduce and Spark
Ethereum is a blockchain-based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mine their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralized, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.

Whilst you would normally need a CLI tool such as GETH to access the Ethereum blockchain, recent tools allow scraping all block/transactions and dump these to csv's to be processed in bulk; notably Ethereum-ETL. These dumps are uploaded daily into a repository on Google BigQuery. We have used this source as the dataset for this coursework.

A subset of the data available on BigQuery is provided at the HDFS folder /data/ethereum. The blocks, contracts, and transactions tables have been pulled down and been stripped of unneeded fields to reduce their size. Please find them attached as csv files. We have also downloaded a set of scams, both active and inactive, run on the Ethereum network via etherscamDB which is available on HDFS at /data/ethereum/scams.json.


**DATASET SCHEMA - BLOCKS**
number: The block number
hash: Hash of the block
miner: The address of the beneficiary to whom the mining rewards were given
difficulty: Integer of the difficulty for this block
size: The size of this block in bytes
gas_limit: The maximum gas allowed in this block
gas_used: The total used gas by all transactions in this block
timestamp: The timestamp for when the block was collated
transaction_count: The number of transactions in the block

**DATASET SCHEMA - TRANSACTIONS**
block_number: Block number where this transaction was in
from_address: Address of the sender
to_address: Address of the receiver. null when it is a contract creation transaction
value: Value transferred in Wei (the smallest denomination of ether)
gas: Gas provided by the sender
gas_price : Gas price provided by the sender in Wei
block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

**DATASET SCHEMA - CONTRACTS**
address: Address of the contract
is_erc20: Whether this contract is an ERC20 contract
is_erc721: Whether this contract is an ERC721 contract
block_number: Block number where this contract was created

**DATASET SCHEMA - SCAMS.JSON**
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


**PART A. TIME ANALYSIS (20%)**
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset. Create a bar plot showing the average value of transactions in each month between the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.
Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)

**PART B. TOP TEN MOST POPULAR SERVICES (25%)**
Evaluate the top 10 smart contracts by total Ether received. An outline of the subtasks required to extract this information is provided below, focusing on a MRJob based approach. This is, however, is not the only way to complete the task, as there are several other viable ways of completing this assignment.

_JOB 1 - INITIAL AGGREGATION_
To workout which services are the most popular, you will first have to aggregate transactions to see how much each address within the user space has been involved in. You will want to aggregate value for addresses in the to_address field. This, in essense, will be similar to the wordcount problem that we saw in Lab 1 and Lab 2.

_JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING_
Once you have obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contracts (example here and slides here). You will want to join the to_address field from the output of Job 1 with the address field of contracts .

Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.

_JOB 3 - TOP TEN_
Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer, utilizing what you have learned from lab 4.


**PART C. TOP TEN MOST ACTIVE MINERS (15%)**
Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.


**PART D. DATA EXPLORATION (40+%)**

_Popular Scams:_ Utilising the provided scam dataset, what is the most lucrative form of scam? Does this correlate with certainly known scams going offline/inactive? For the correlation, you could produce the count of how many scams for each category are active/inactive/offline/online/etc and try to correlate it with volume (value) to make conclusions on whether state plays a factor in making some scams more lucrative. Therefore, getting the volume and state of each scam, you can make a conclusion whether the most lucrative ones are ones that are online or offline or active or inactive. So for that purpose, you need to just produce a table with SCAM TYPE, STATE, VOLUME which would be enough (15%).

_Wash Trading:_ Wash trading is defined as "Entering into or purporting to enter into, transactions to give the appearance that purchases and sales have been made, without incurring market risk or changing the trader’s market position" Unregulated exchanges use these to fake up to 70% of their trading volume? Which addresses are involved in wash trading? Which trader has the highest volume of wash trades? How certain are you of your result? More information can be found at https://dl.acm.org/doi/pdf/10.1145/3442381.3449824. One way to measure ether balance over time is also possible but you will need to discuss accuracy concerns. (20%)

_Fork the Chain:_ There have been several forks of Ethereum in the past. Identify one or more of these and see what effect it had on price and general usage. For example, did a price surge/plummet occur, and who profited most from this? (10%)

_Gas Guzzlers:_ For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? Also, could you correlate the complexity for some of the top-10 contracts found in Part-B by observing the change over their transactions (10%)

_Comparative Evaluation_ Reimplement Part B in Spark (if your original was MRJob, or vice versa). How does it run in comparison? Keep in mind that to get representative results you will have to run the job multiple times, and report median/average results. Can you explain the reason for these results? What framework seems more appropriate for this task? (10%)
