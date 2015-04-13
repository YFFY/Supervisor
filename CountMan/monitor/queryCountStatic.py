#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import gzip
from CountMan.monitor.util import *

class queryStatic(object):

    def __init__(self):
        self.staticday = getDate()[STATISTICSDAY]
        self.dber = DatabaseInterface()
        self.querycount = dict()
        self.get_file()
        self.get_count()

    def get_file(self):
        fileName = 'realquery.log.{0}.gz'.format(self.staticday)
        self.filePath = os.path.join(QUERY_COUNT_PATH, fileName)

    def get_count(self):
        count = 0
        with gzip.open(self.filePath, 'r') as fr:
            for line in fr:
                if 'query cost' in line:
                    count += 1
        self.querycount['querycount'] = count

    @property
    def set2db(self):
        if self.dber.connectionStatus:
            self.dber.insertCollection(self.querycount)

if __name__ == '__main__':
    qs = queryStatic()
    qs.set2db
