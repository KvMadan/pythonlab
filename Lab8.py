import csv
import json
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseTransientError

from couchbase.bucket import Bucket

cb = Bucket('couchbase://192.168.61.101/pysample?operation_timeout=30')

# for row in csv_o:
#     cb.upsert('{0}'.format(row['id']), row)
#     cb.upsert_multi({
#         '{0}'.format("ITEM1::" + row['id']) : {"shirt" : "location"} ,
#         '{0}'.format("ITEM2::" + row['id']) : {"pants" : "location"} ,
#         '{0}'.format("ITEM3::" + row['id']) : {"shoe" : "location"}
#     })


batches = []
cur_batch = []
batch_size = 1000
x = 0
y = 0
# thebatch.append(cur_batch)

csvfile = open('Crimes_-_2001_to_present.csv', 'r')
# jsonfile = open('file.json', 'w')

fieldnames = ("ID","Case Number","Date","Block","IUCR","Primary Type","Description","Location Description","Arrest","Domestic","Beat","District","Ward","Community Area","FBI Code","X Coordinate","Y Coordinate","Year","Updated On","Latitude","Longitude","Location")
reader = csv.reader(csvfile, fieldnames)

for row in reader:
    cur_batch[row] = reader[row]
    cur_size += len(cur_batch)
    if cur_size > batch_size:
            cur_batch = {}
            batches.append(cur_batch)
            cur_size = 0

print "Have {} batches".format(len(batches))

num_completed = 0
while batches:
    batch = batches[-1]
    try:
        cb.upsert_multi(batch)
        num_completed += len(batch)
        cb.pop()
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

print "Bye"

rownum = 0
for row in reader:
    # Save header row.
    if rownum == 0:
        header = row
    else:
        colnum = 0
        for col in row:
            print '%-8s: %s' % (header[colnum], col)
            colnum += 1

    rownum += 1

csvfile.close()
cb.close()

from flask import Flask

app = Flask(__name__)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()