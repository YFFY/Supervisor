#! /usr/bin/env python
# --*-- coding:utf-8 --*--


import os
import sys
sys.path.append(os.path.split(os.path.abspath(sys.path[0]))[0])

from CountMan.util import getBrokerQueryResult, DatabaseInterface

class Broker(object):

    def __init__(self):
        self.dber = DatabaseInterface()

    def getBrokerResult(self):
        self.result = getBrokerQueryResult()

    @property
    def set2db(self):
        self.getBrokerResult()
        if self.dber.connectionStatus:
            self.dber.insertCollection(self.result)

if __name__ == '__main__':
    b = Broker()
    b.set2db