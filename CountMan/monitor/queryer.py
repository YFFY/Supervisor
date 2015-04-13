#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

from CountMan.monitor.util import *
from CountMan.monitor.setting import *

class Queryer(object):

    def __init__(self):
        self.dao = DatabaseInterface()
        self.dataSet = dict()
        self.logger = getLogger('root')

    def getData(self):
        for queryKey in QUERYPARAM:
            self.dataSet[queryKey] = getResponse(QUERYPARAM.get(queryKey))

    @property
    def set2db(self):
        self.getData()
        self.logger.info('get query data: {0} success'.format(self.dataSet))
        self.dao.insertCollection(self.dataSet)

if __name__ == '__main__':
    q = Queryer()
    if ISDEBUG:
        import cProfile
        cProfile.run("q.set2db")
    else:
        q.set2db
