'''
    read bronze json file
    modeling into a more optimized table and creating data quality checks
    save gold parquet
'''
#%% 
from pymongo import MongoClient
from datetime import datetime
from bson import json_util
import json
import os

def fileExists(file):
    '''
    verify if file with the string name specified exists on the directory
    ------------
    return: bool
    '''
    if os.path.isfile(file) and os.access(file, os.R_OK):
        return True
    else:
        return False

def try_parsing_date(text):
    '''
    check and transform specified str to date object if it matches with any format available
    parameters:
    ------------
    text: str

    return: datetime.datetime
    '''
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')

def init():

    client = MongoClient("mongodb://127.0.0.1:27017")
    mydb = client["local"]
    mycol = mydb["chatBot_feed"]

    if fileExists("result.json"):
        #get the last date from result.json to use as checkpoint
        print("result.json already exists.")
        f = open('result.json')
        data = json.load(f)
        last_doc = data[-1]
        date = last_doc.get('created_at')
        date_str = date.get('$date')
        date_obj = try_parsing_date(date_str)

        cursor = mycol.find({'created_at': {'$gt': date_obj}})
    else:
        print("result.json doesn't exist.")
        cursor = mycol.find({})

    array_documents = []
    for documents in cursor:
        array_documents.append(documents)

    filename = "result.json"
    with open(filename, mode='w') as f:
        f.write(json_util.dumps(array_documents))
    print("result.json imported from MongoDB successfully.")

if __name__ == "__main__":
    init()

