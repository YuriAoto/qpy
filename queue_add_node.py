#!/usr/bin/python
# queue_master_driver.py
#import argparse
from multiprocessing.connection import Client
from time import sleep
import os
import sys

address = 'localhost'
port = 16011
key = 'qwerty'

master = Client( (address, port), authkey=key)

if (len( sys.argv) != 3):
    sys.quit( 'Usage: <node address> <max_jobs>')

node_address = sys.argv[1]
node_max_jobs = int( sys.argv[2])

master.send( (2, ( node_address, node_max_jobs)))
print master.recv()

master.close()
