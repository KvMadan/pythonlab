import csv
from couchbase.bucket import Bucket

cb = Bucket('couchbase://localhost/streamer')
o = open ('user_list.csv','rU')
csv_o = csv.DictReader(o)
for row in csv_o:
    cb.upsert('{0}'.format(row['id']), row)
    cb.upsert_multi({
        '{0}'.format("ITEM1::" + row['id']) : {"shirt" : "location"} ,
        '{0}'.format("ITEM2::" + row['id']) : {"pants" : "location"} ,
        '{0}'.format("ITEM3::" + row['id']) : {"shoe" : "location"}
    })
o.close()
print '****************'
print 'Touch Mels feet'
print '****************'
from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
