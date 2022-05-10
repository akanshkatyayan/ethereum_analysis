import json
import pandas as pd

# Read JSON file
inputfile = open('scams.json')
json_data = json.load(inputfile)
 
# Convert JSON data to Pandas Dataframe
df_scams = pd.DataFrame(columns=['Address', 'Scam Type', 'Status'])
keys = json_data["result"]

# Check for all the records in JSON
for i in keys:
  record = json_data["result"][i]
  addresses = record["addresses"]
  category = record["category"]
  status = record["status"]

  # Loop to check all addresses within each scam

  for address in addresses:
    df_scams.loc[len(df_scams)] = [address, category, status]

df_scams.to_csv('scams.csv', index=False)