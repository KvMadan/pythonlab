import datetime
import pytz
import commands
import time

from couchbase.bucket import Bucket

# Easy way to use the right path for Couchbase binaries
# binPath = "C:\Program Files\Couchbase Server"
binPath = "/Applications/couchbase-server-enterprise_4/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin"
# binPath = "/opt/couchbase/bin"
seedNode = "192.168.61.101"

cb = Bucket('couchbase://' + seedNode + '/testload?operation_timeout=30')

loopControl = commands.getoutput('cat ~/keepAlive')

while not (loopControl == "false"):

    QueueSize = commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 -b testload all -j |jq .ep_queue_size')
    TodoSize = commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 -b testload all -j |jq .ep_flusher_todo')
    diskDrain = int(QueueSize) + int(TodoSize)

    flushFail = int(commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 -b testload all -j |jq .ep_item_flush_failed'))
    tempOOM = int(commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 memory -b testload -j |jq .ep_tmp_oom_errors'))
    cacheMiss = int(commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 all -b testload -j |jq .ep_bg_fetched'))
    memUsed = int(commands.getoutput(binPath + '/cbstats ' + seedNode + ':11210 all -b testload -j |jq .mem_used'))

    # Add other REST stats
    # opsPer = commands.getoutput('curl -u Administrator:password http://192.168.61.101:8091/pools/default/buckets/testload/stats |jq .op.samples.ops')

    # Iterate across cluster for node health
    # numNodes = commands.getoutput("curl -u Administrator:password http://" + seedNode + ":8091/pools/default |jq '.nodes | length'")
    # nodeHealth = []
    # for i in range(0,numNodes):
    #    nodeHealth[i] = commands.getoutput("curl -u Administrator:password http://" + seedNode + ":8091/pools/default |jq .nodes[" + i + "].status")

    #nowStamp = datetime.datetime.now(tz=pytz.UTC)
    nowStamp = datetime.datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')

    json_str = {
        'type': "stats",
        'flush': flushFail,
        'drain': diskDrain,
        'OOM': tempOOM,
        'miss': cacheMiss,
        'memory': memUsed
    }

    cb.upsert(nowStamp, json_str, ttl=2505600)

    time.sleep(1)
    loopControl = commands.getoutput('cat ~/keepAlive')
    print("keep going = " + loopControl)

cb.closed

