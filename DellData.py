import sqlite3
import json
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseTransientError

bucket = Bucket('couchbase://127.0.0.1/testload?operation_timeout=30')
rdbms = sqlite3.connect('Dell-data-0000.cbb')

cursor = rdbms.execute('select key, val from cbb_msg')
testrecord =  cursor.fetchone()
print testrecord[0]
print testrecord[1]

cursor = rdbms.execute('select key, val from cbb_msg')
for row in cursor:
    try:
        bucket.upsert(str(row[0]), json.loads(str(row[1])))
    except:
        print(str(row[0]) + " upsert failed.")
        pass
