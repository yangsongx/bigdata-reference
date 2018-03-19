#!/usr/bin/env python

#coding:utf-8


import socket
import time
import json
import re
import sys

reload(sys)  
sys.setdefaultencoding('utf8') 

def server_side():
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 9998))
    s.listen(5)
    conn,addr = s.accept()
    while True:
        conn.send("hello world\n")
        print("Sent the words")
        time.sleep(2)
    return 0

# 1 - return valid address, 0 otherwise
def parse_address_section(indata):
    ret = 0
    jsdata = json.loads(indata)
    if jsdata.has_key('events'):
        eventdata = json.loads(jsdata['events'])
        print eventdata
        print("the event data list length=%d" %(len(eventdata)))
        for it in eventdata:
            if it.has_key("segmentation") and it['segmentation'].has_key('address'):
                print("the address:%s" %(it['segmentation']['address']))
                ret = 1
                break;
    return ret

def dump_data_entry(fn):
    fd = open('./new2.txt','w')
    with open(fn, 'r') as f:
        for line in f:
            jsline = re.search(r'\{\".*\"}', line)
            if jsline != None:
                data = jsline.group(0)
                jsdata = json.loads(data)
                if jsdata.has_key('events'):
                    eventdata = json.loads(jsdata['events'])
                    for it in eventdata:
                        if it.has_key("segmentation") and it['segmentation'].has_key('address'):
#print("re")
#print it['segmentation']['address']
                            addr = re.search(r"province\s{0,}:(.*)city\s{0,}:(.*?)\n", it['segmentation']['address'], re.S)
                            if addr != None:
                                print("group - 1 = %s" %(addr.group(1).strip()))
                                print("group - 2 = %s" %(addr.group(2)))
                                fd.write(line)
                                break
                            else:
                               print "PITY-%s" %(line)
    fd.close()
    return 0

## below is just a util to extract big data
#  then stored target data in new file
def extract_test_section(fn, newfn):
    index = 0
    fd = open(newfn, 'w')

    with open(fn, 'r') as f:
        for line in f:
            index = index + 1
            print line
            jsline = re.search(r'\{\".*\"}', line)
            if jsline != None:
                print "============="
                print jsline.group(0)
                print ("++++++++++++++")
                if parse_address_section(jsline.group(0)) == 1:
                        fd.write(line)
            else:
                print "PITY -  %s" %(line)
    fd.close()

    return 0
###########################
# Wrapper class for auto-test
class AddressGenerator:
    __src = "default.txt"

    def __init__(self, src):
        self.__src = src

    def extract_js_section(self, inputline):
        jsline = re.search(r'\{\".*\"}', inputline)
        if jsline != None:
            return jsline.group(0)
        else:
            return ""

    def parse_addr_in_events(self, evt_list):
        province = ""
        city = ""
        for it in evt_list:
            if it.has_key("segmentation") and it['segmentation'].has_key('address'):
                addr = re.search(r"province\s{0,}:(.*)city\s{0,}:(.*?)\n", \
                        it['segmentation']['address'], \
                        re.S)
                if addr != None:
                    province = addr.group(1).strip()
                    city = addr.group(2)
                    break; # one-shot
        return (province, city)

    def where(self, inputline):
        jsline = self.extract_js_section(inputline)
        jsdata = json.loads(jsline)
        if jsdata.has_key('events'):
            eventdata = json.loads(jsdata['events'])
            (prov, city) = self.parse_addr_in_events(eventdata)
            print("%s - %s" %(prov, city))
        return 0

    def launch(self):
        print("Now, using %s as input source" %(self.__src))

        with open(self.__src, "r") as f:
            for line in f:
                self.where(line)
        return 0

##########################################################################################
#server_side()
#extract_test_section('/home/ysx/test.data.countly', './new.txt')

#dump_data_entry('./new.txt')
obj = AddressGenerator('./data.txt')
obj.launch()


