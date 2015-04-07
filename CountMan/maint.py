#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.abspath(sys.path[0]))[0])


import json
import traceback
from CountMan.setting import *
from CountMan.errcode import *
from CountMan.util import DatabaseInterface, getHtmlContent, Emailer, getEmailTitle, setLog


class Parser(object):

    def __init__(self):
        self.dao = DatabaseInterface()
        self.rawResult = self.dao.getQueryResult()

    def getParserResult(self):
        if not self.rawResult:
            print 'get empty result from db'
        try:
            collectorList = list()
            queryList = list()
            impalaList = list()
            brokerList = list()
            for result in self.rawResult:
                if "ip" in result:
                    setLog("get collector: {0}".format(result))
                    collectorList.append(result)
                elif "YMCLICK" in result:
                    setLog("get query: {0}".format(result))
                    queryList.append(result)
                elif "impalaClick" in result:
                    setLog("get impala: {0}".format(result))
                    impalaList.append(result)
                else:
                    setLog("get broker: {0}".format(result))
                    brokerList.append(result)
        except Exception as ex:
            traceback.print_exc()
        if len(collectorList) != REALTIME_NODE_COUNT:
            return REALTIME_DATA_NOT_ENOUGH
        if not queryList:
            return QUERY_DATA_IS_NONE
        setLog("get html content success")
        return getHtmlContent(collectorList, queryList, impalaList, brokerList)


class Sender(object):

    def __init__(self):
        self.parser = Parser()
        self.emailer = Emailer()
        self.dber = DatabaseInterface()

    @property
    def sendEmail(self):
        htmlContent = self.parser.getParserResult()
        sendStatus = self.emailer.sendMessage(getEmailTitle(), htmlContent)
        if sendStatus:
            self.dber.deleteRecord()


if __name__ == '__main__':
    s = Sender()
    s.sendEmail