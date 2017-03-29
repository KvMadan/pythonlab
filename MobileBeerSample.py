import json
import time

from couchbase.bucket import Bucket
import requests
from couchbase.views.iterator import View
from collections import Counter
import numpy as np

CBS="10.112.151.101"
SG="10.112.151.101"
CBG="10.112.151.1"
cb = Bucket('couchbase://10.112.151.101/beer-sample?operation_timeout=30')
cb2 = Bucket('couchbase://10.112.151.101/beers?operation_timeout=30')

i = 0
start = 0
end = 0
avg_track = []
headers = {'content-type': 'application/json'}

for result in View(cb, "beer", "allkeys"):
    jsonID = result.docid
    payload = cb.get(jsonID).value
    jsonKey = ("SG::" + str(i))
    start = time.time()
    resp = requests.post('http://{}:4984/beers/'.format(CBS), data=json.dumps(payload), headers=headers)
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average SG POST ... ")
print (avg_value)
modedata = Counter(avg_track)
#print (modedata.most_common())   # Returns all unique items and their counts
print (modedata.most_common(1))  # Returns the highest occurring item
print (np.percentile(avg_track, 95))

i = 0
start = 0
end = 0
avg_track = []

for result in View(cb2, "beer", "allkeys"):
    jsonID = result.docid
    start = time.time()
    resp = requests.get('http://{}:4984/beers/'.format(SG), data=json.dumps(jsonID), headers=headers)
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average SG GET ...")
print(avg_value)
modedata = Counter(avg_track)
#print(modedata.most_common())   # Returns all unique items and their counts
print(modedata.most_common(1))  # Returns the highest occurring item
print (np.percentile(avg_track, 95))

for result in View(cb2, "beer", "allkeys"):
    jsonID = result.docid
    theStatement = "statement=SELECT%20*%20from%20%60beer-sample%60%20USE%20KEYS%20%22" + jsonID + "%22"
    start = time.time()
    resp = requests.get('https://{}:18093/query?'.format(SG), data=theStatement, headers=headers, verify='/Users/justin/Documents/Demo/pythonlab/CBcert.ca')
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average N1QL GET ...")
print(avg_value)
modedata = Counter(avg_track)
#print(modedata.most_common())   # Returns all unique items and their counts
print(modedata.most_common(1))  # Returns the highest occurring item
print (np.percentile(avg_track, 95))

for result in View(cb2, "beer", "allkeys"):
    jsonID = result.docid
    start = time.time()
    resp = requests.get('http://{}:8888/GETit/'.format(CBG), data=jsonID, headers=headers)
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average RESTful GET ...")
print(avg_value)
modedata = Counter(avg_track)
#print(modedata.most_common())   # Returns all unique items and their counts
print(modedata.most_common(1))  # Returns the highest occurring item
print (np.percentile(avg_track, 95))

cb.closed
