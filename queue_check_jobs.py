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
master.send( (5, ()))
print master.recv()
master.close()
