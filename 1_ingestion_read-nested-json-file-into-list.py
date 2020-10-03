# Read Newline delimited json file into a list
# in case the data is really nested and has a lot of records
import json

file = []
for record in open('Account_2020-09-15.json', 'r'):
    file.append(json.loads(record))
    
print(file)
