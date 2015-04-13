#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

import traceback
from CountMan.monitor.setting import *
from CountMan.monitor.errcode import *
from CountMan.monitor.util import DatabaseInterface, getHtmlContent, Emailer, getEmailTitle, getLogger


class Parser(object):

    def __init__(self):
        self.dao = DatabaseInterface()
        self.logger = getLogger("root")
        self.rawResult = self.dao.getQueryResult()

    def getParserResult(self):
        if not self.rawResult:
            print 'get empty result from db'
        try:
            collectorList = list()
            queryList = list()
            impalaList = list()
            querycountlist = list()
            brokerList = list()
            for result in self.rawResult:
                if "ip" in result:
                    self.logger.info("get collector: {0}".format(result))
                    collectorList.append(result)
                elif "YMCLICK" in result:
                    self.logger.info("get query: {0}".format(result))
                    queryList.append(result)
                elif "impalaClick" in result:
                    self.logger.info("get impala: {0}".format(result))
                    impalaList.append(result)
                elif "querycount" in result:
                    self.logger.info("get query count: {0}".format(result))
                    querycountlist.append(result)
                else:
                    self.logger.info("get broker: {0}".format(result))
                    brokerList.append(result)
        except Exception as ex:
            traceback.print_exc()
        if len(collectorList) != REALTIME_NODE_COUNT:
            return REALTIME_DATA_NOT_ENOUGH
        if not queryList:
            return QUERY_DATA_IS_NONE
        self.logger.info("get html content success")
        return getHtmlContent(collectorList, queryList, impalaList, brokerList, querycountlist)


class Sender(object):

    def __init__(self):
        self.parser = Parser()
        self.emailer = Emailer()
        self.dber = DatabaseInterface()
        self.logger = getLogger('root')

    @property
    def sendEmail(self):
        htmlContent = self.parser.getParserResult()
        sendStatus = self.emailer.sendMessage(getEmailTitle(), htmlContent)
        if sendStatus:
            self.logger.info("send email success")
        else:
            self.logger.info("send email failed")
        self.dber.deleteRecord()
        self.logger.info("delete record success")


if __name__ == '__main__':
    s = Sender()
    s.sendEmail