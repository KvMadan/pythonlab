import json
import time

from couchbase.bucket import Bucket
import requests
from couchbase.views.iterator import View
from collections import Counter
import numpy as np

cb = Bucket('couchbase://10.112.151.101/beer-sample?operation_timeout=30')
cb2 = Bucket('couchbase://10.112.151.101/beers?operation_timeout=30')
i = 0
start = 0
end = 0
avg_track = []

for result in View(cb, "beer", "allkeys"):
    jsonID = result.docid
    payload = cb.get(jsonID).value
    headers = {'content-type': 'application/json'}
    jsonKey = ("SG::" + str(i))
    start = time.time()
    resp = requests.post('http://10.112.151.101:4984/beers/', data=json.dumps(payload), headers=headers)
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average POST ... ")
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
    resp = requests.get('http://10.112.151.101:4984/beers/', data=json.dumps(jsonID), headers=headers)
    end = time.time()
    avg_track.extend([(end - start) * 1000])
    #time.sleep(2)
    i = i + 1

avg_value = sum(avg_track)/len(avg_track)
#print(avg_track)
print("average GET ...")
print(avg_value)
modedata = Counter(avg_track)
#print(modedata.most_common())   # Returns all unique items and their counts
print(modedata.most_common(1))  # Returns the highest occurring item
print (np.percentile(avg_track, 95))

cb.closed
