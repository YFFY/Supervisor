#! /usr/bin/env python
# --*-- coding:utf-8 --*--

from CountMan.util import *
from CountMan.setting import *

class Queryer(object):

    def __init__(self):
        self.dao = DatabaseInterface()
        self.dataSet = dict()

    def getData(self):
        for queryKey in QUERYPARAM:
            self.dataSet[queryKey] = getResponse(QUERYPARAM.get(queryKey))

    @property
    def set2db(self):
        self.getData()
        self.dao.insertCollection(self.dataSet)

if __name__ == '__main__':
    q = Queryer()
    if ISDEBUG:
        import cProfile
        cProfile.run("q.set2db")
    else:
        q.set2db
