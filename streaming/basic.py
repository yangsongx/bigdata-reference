#!/usr/bin/env python

#coding:utf-8
from __future__ import print_function
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.streaming import StreamingContext
import pprint

def window_operation_study(indata):
    wcnt = indata.reduceByKeyAndWindow(lambda x, y: x + y,12, 6)
    wcnt.pprint()
    return 0
def dbg_output(item):
    print("\n==== begin ====")
    print(item.first())
    print("~~the totally count in the rdd=%d" %(item.count()))
    print("the debug output\n")
    return 0

def tcp_streaming_study():
    # every 6 seconds
    sc = SparkContext()
    ssc = StreamingContext(sc, 6)
    dobjs= ssc.socketTextStream("127.0.0.1",\
                          9998)
    ssc.checkpoint("/tmp/ysx")
                  
    dobjs.pprint()
    counts = dobjs.flatMap( lambda line: line.split(" "))\
             .map(lambda word: (word, 1))\
             .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    print("\n\n==== data calculate ===\n\n")
#wcnt = counts.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 12, 6)
    wcnt = counts.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 12, 6)
    wcnt.pprint()
    print("#################\n")
    wcnt.foreachRDD(dbg_output)

    wcnt2 = counts.countByWindow(12, 6)
    wcnt2.pprint()
    wcnt2.count().pprint()

    ssc.start()
    ssc.awaitTermination()

    return 0

### main ###
#ssc =  StreamingContext.getOrCreate("/tmp/ysx", tcp_streaming_study)
#ssc.start()
#ssc.awaitTermination()
tcp_streaming_study()

