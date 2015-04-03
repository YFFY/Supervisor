#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import gzip
import traceback
from CountMan.util import *
from CountMan.setting import *
from CountMan.errcode import *

class Counter(object):

    def __init__(self):
        self.statisticsDay = getDate()[STATISTICSDAY]
        self.localip = getLocalIp()
        self.dao = DatabaseInterface()
        self.realtimeDir = REALTIMEDIR
        self.realtimeTable = {
            "date": self.statisticsDay,
            "ip": self.localip,
            "ymds_druid_datasource" : 0,
            "ymds_druid_datasource0" : 0,
            "ymds_druid_datasource4" : 0,
            "ymds_druid_datasource6" : 0,
        }

    def getCount(self):

        fileList = [os.path.join(self.realtimeDir, fileName)
                    for fileName in os.listdir(self.realtimeDir)
                    if self.statisticsDay in fileName and fileName.endswith('.tar.gz')]
        if not fileList:
            return NO_FILE_FOUND_IN_DIR
        for absFile in fileList:
            singleCount = 0
            with gzip.open(absFile, 'r') as fz:
                for line in fz:
                    if 'isThrow:false' in line:
                        singleCount += 1
            datasource = '_'.join(os.path.split(absFile)[1].split('-')[0].split('_')[:3]).replace('.', '')
            try:
                self.realtimeTable[datasource] += singleCount
            except KeyError as ex:
                traceback.print_exc()

    @property
    def set2db(self):
        self.getCount()
        self.dao.insertCollection(self.realtimeTable)

if __name__ == '__main__':
    c = Counter()
    if ISDEBUG:
        import cProfile
        cProfile.run("c.set2db")
    else:
        c.set2db