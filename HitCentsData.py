import sqlite3
import json
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseTransientError

bucket = Bucket('couchbase://192.168.4.81/DELL?operation_timeout=30')
rdbms = sqlite3.connect('data-0000.cbb')

cursor = rdbms.execute('select key, val from cbb_msg')
testrecord =  cursor.fetchone()
print testrecord[0]
print testrecord[1]

# cursor = rdbms.execute('select key, val from cbb_msg')
# for row in cursor:
#     try:
#         bucket.upsert(str(row[0]), json.loads(str(row[1])))
#     except:
#         print(str(row[0]) + " upsert failed.")
#         pass

BYTES_PER_BATCH = 1024 * 256 # 256K

# Generate our data:
all_data = {}
cursor = rdbms.execute('select key, val from cbb_msg')
for row in cursor:
    print "key" + str(row[0])
    print "value" + str(row[1])
    key = str(row[0])
    value = json.loads(str(row[1]))
    all_data[key] = value

batches = []
cur_batch = {}
cur_size = 0
batches.append(cur_batch)

for key, value in all_data.items():
    cur_batch[key] = value
    cur_size += len(key) + len(value) + 24
    if cur_size > BYTES_PER_BATCH:
        cur_batch = {}
        batches.append(cur_batch)
        cur_size = 0

print "Have {} batches".format(len(batches))

num_completed = 0
while batches:
    batch = batches[-1]
    try:
        bucket.upsert_multi(batch)
        num_completed += len(batch)
        batches.pop()
    except CouchbaseTransientError as e:
        print e
        ok, fail = e.split_results()
        new_batch = {}
        for key in fail:
            new_batch[key] = all_data[key]
        batches.pop()
        batches.append(new_batch)
        num_completed += len(ok)
        print "Retrying {}/{} items".format(len(new_batch), len(ok))

print "Completed {}/{} items".format(num_completed, len(all_data))