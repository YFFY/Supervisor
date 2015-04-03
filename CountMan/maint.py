#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import json
import traceback
from CountMan.setting import *
from CountMan.errcode import *
from CountMan.util import DatabaseInterface, getHtmlContent, Emailer, getEmailTitle


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
            for result in self.rawResult:
                if "ip" in result:
                    collectorList.append(result)
                elif "YMCLICK" in result:
                    queryList.append(result)
                else:
                    impalaList.append(result)
        except Exception as ex:
            traceback.print_exc()
        if len(collectorList) != REALTIME_NODE_COUNT:
            return REALTIME_DATA_NOT_ENOUGH
        if not queryList:
            return QUERY_DATA_IS_NONE
        return getHtmlContent(collectorList, queryList, impalaList)


class Sender(object):

    def __init__(self):
        self.parser = Parser()
        self.emailer = Emailer()

    def getHtmlContent(self):
        htmlContent = self.parser.getParserResult()
        self.emailer.sendMessage(getEmailTitle(), htmlContent)


if __name__ == '__main__':
    s = Sender()
    s.getHtmlContent()