#!/usr/bin/env python

#coding:utf-8


import socket
import time

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

server_side()

