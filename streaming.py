#!/usr/bin/python
#from __future__ import print_funcction
import sys
import pprint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def simple_test_kafka():
    print("coming the function")
    sc = SparkContext(appName="MyApp")
    batchIntervalSec=10
    windowIntervalSec=36000
    ssc = StreamingContext(sc, batchIntervalSec)
    print("hello world, context created")
    kvs = KafkaUtils.createStream(ssc, '172.20.20.65:2181', 'hello',  {'countly': 2})
    print("\n\n===Got data:\n")
    print kvs
    print("\n")
    kvs.pprint()
    kvs_2 = kvs.map(lambda v: v[1])
    kvs_2.pprint()
#  items_dstream = kvs.map(lambda v: v.split(","))
#    items_dstream.pprint()
    print("==== END output")
#inbound_batch_cnt = items_dstream.count()
#    inbound_window_cnt = items_dstream.countByWindow(windowIntervalSec,batchIntervalSec)
    ssc.start()
    ssc.awaitTermination()
    return 0

def simple_test_tcp():
    print("coming the tcp function")
    sc = SparkContext(appName="TcpApp")
    windowIntervalSec=36000
    ssc = StreamingContext(sc, 10) # every 10 seconds
    print("tcp case, context created")
#kvs = KafkaUtils.createStream(ssc, '172.20.20.65:2181', 'hello',  {'szjy': 2})
    lines = ssc.socketTextStream("127.0.0.1", 9998)
    print("\n\n===Got data:\n")
    print lines
    print("\n")
    lines.pprint()
    print("==== END output")
#inbound_batch_cnt = items_dstream.count()
#    inbound_window_cnt = items_dstream.countByWindow(windowIntervalSec,batchIntervalSec)
    ssc.start()
    ssc.awaitTermination()
    return 0
### main ##
print("entry point")
simple_test_kafka()
#simple_test_tcp()
