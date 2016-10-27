import datetime
import pytz
import commands
import time
import requests

from couchbase.bucket import Bucket

# Easy way to use the right path for Couchbase binaries
# binPath = "C:\Program Files\Couchbase Server"
binPath = "/Applications/couchbase-server-enterprise_4/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin"
# binPath = "/opt/couchbase/bin"

# This is the cluster we'll use to collect statistics
seedNode = "192.168.61.101"
# This is the cluster we're monitoring.
# We'll use the localhost to determine which node in the cluster this script is running.
# clusterNode = "localhost"
clusterNode = "192.168.61.101"

# Iterate across cluster for the node I'm on
numNodes = int(commands.getoutput("curl -s -u Administrator:password http://" + clusterNode + ":8091/pools/default |jq '.nodes | length'"))
for i in range(0,numNodes-1):
    ctr = (str(commands.getoutput("curl -s -u Administrator:password http://" + clusterNode + ":8091/pools/default |jq .nodes[" + str(i) + "].thisNode")))
    #print (ctr)
    if ctr == "true":
        thisNode = (str(commands.getoutput("curl -s -u Administrator:password http://" + clusterNode + ":8091/pools/default |jq .nodes[" + str(i) + "].otpNode")))
        thisNode = thisNode.split("@")[1]
        thisNode = thisNode.split("\"")[0]
        #print ("this node" + str(thisNode))

cb = Bucket('couchbase://' + seedNode + '/testload?operation_timeout=30')
print (thisNode)

loopControl = commands.getoutput('cat ~/keepAlive')

while not (loopControl == "false"):

    QueueSize = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 -b testload all -j |jq .ep_queue_size'))
    TodoSize = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 -b testload all -j |jq .ep_flusher_todo'))
    diskDrain = int(QueueSize) + int(TodoSize)

    flushFail = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 -b testload all -j |jq .ep_item_flush_failed'))
    tempOOM = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 memory -b testload -j |jq .ep_tmp_oom_errors'))
    cacheMiss = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 all -b testload -j |jq .ep_bg_fetched'))
    memUsed = int(commands.getoutput(binPath + '/cbstats ' + str(thisNode) + ':11210 all -b testload -j |jq .mem_used'))

    # Add other REST stats
    opsPer = int(commands.getoutput("curl -s -u Administrator:password http://" + str(thisNode) + ":8091/pools/default/buckets/testload/stats |jq .op.samples.ops[0]"))
    #opsPer = opsStat.split("\"")[0]
    healthStat = str((commands.getoutput("curl -s -u Administrator:password http://" + str(seedNode) + ":8091/pools/default |jq .nodes[" + str(i) + "].status")))
    nodeHealth = healthStat.split("\"")[1]

    # Drain Queue via REST API
    # diskDrain = commands.getoutput("curl -s -u Administrator:password http://" + seedNode + ":8091/pools/default/buckets/testload/stats |jq .op.samples.ep_diskqueue_drain[59]")
    resp = requests.get('http://' + str(thisNode) + ':8091/pools/default/buckets/testload/stats')
    if resp.status_code != 200:
        # This means something went wrong.
        print("oh crap" + resp.status_code)
    for theItems in resp.json():
        print("output" + str(theItems))

    #nowStamp = datetime.datetime.now(tz=pytz.UTC)
    nowStamp = datetime.datetime.now(tz=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')

    json_str = {
        'type': "stats",
        'flush': flushFail,
        'drain': diskDrain,
        'OOM': tempOOM,
        'miss': cacheMiss,
        'memory': memUsed,
        'operations': opsPer,
        'nodes': nodeHealth
    }

    cb.upsert(str(thisNode) + "::" + nowStamp, json_str, ttl=2505600)

    time.sleep(1)
    loopControl = commands.getoutput('cat ~/keepAlive')
    print("keep going = " + nowStamp)

cb.closed

