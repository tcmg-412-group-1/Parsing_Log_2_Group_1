from urllib.request import urlretrieve
import os
import re
import collections 

response=0

URL = 'https://s3.amazonaws.com/tcmg476/http_access_log'
LOCAL_FILE = 'http_access_log'

def file_len(LOCAL_FILE):
    with open (LOCAL_FILE) as f:
        for response, l in enumerate (f):
            pass
    return response + 1

SumRequest = file_len(LOCAL_FILE)

print("Total HTTP Request Made: ", file_len(LOCAL_FILE))
print("Resquest per month:", round(SumRequest/12))
print("Request per week: ",round(SumRequest/52))
print("Resquest per day: ", round(SumRequest/365))