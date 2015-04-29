#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.abspath(sys.path[0]))[0])


import time
import requests
import pymongo
import copy
import datetime
import json
import logging
import logging.config
from string import Template
import socket
import traceback
import smtplib, sys
from CountMan.monitor.setting import *
from CountMan.monitor.errcode import *
from email.mime.text import MIMEText


def getLocalIp():
    localIP =  socket.gethostbyname(socket.gethostname()).replace('.', '-')
    if localIP == "127-0-0-1":
        tempString = os.popen('/sbin/ifconfig').readlines()
        localIP = tempString[1].split()[1].split(':')[1].replace('.', '-')
    return localIP

def getDate():
    today = datetime.datetime.now()
    yesterday = today + datetime.timedelta(days=-1)
    return yesterday.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def datetime2timestamp(dt):
     s = time.mktime(time.strptime(dt, '%Y-%m-%d'))
     return int(s)

def getTimestamp():
    start, end = getDate()
    return datetime2timestamp(start), datetime2timestamp(end)

def getHourTimestamp():
    currentHour = datetime.datetime.now()
    previousHour = currentHour + datetime.timedelta(hours=-1)
    return int(time.mktime(time.strptime(previousHour.strftime('%Y-%m-%d %H'), '%Y-%m-%d %H'))),int(time.mktime(time.strptime(currentHour.strftime('%Y-%m-%d %H'), '%Y-%m-%d %H')))


def getOfferIdList():
    offList = list()
    param = '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["offer_id"],"data":["conversion"],"filters":{"$and":{}},"sort":[]}'
    end = datetime.datetime.now()
    start = end + datetime.timedelta(hours=-1)
    unixstart = int(time.mktime(time.strptime(start.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:00:00')))
    unixend = int(time.mktime(time.strptime(end.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:00:00')))
    requestUrl = (QUERYURL + param) % (unixstart, unixend)
    r = requests.get(requestUrl)
    if r.status_code == 200:
        content = r.text
    else:
        return offList
    if isinstance(content, dict):
        dataList = content.get('data').get('data')
        for data in dataList:
            offList.append(data[0])
    else:
        try:
            jsonContent = json.loads(content)
            dataList = jsonContent.get('data').get('data')
            for data in dataList[1:]:
                offList.append(data[0])
        except Exception as ex:
            return offList
    return offList


def getSplitOffers():
    offerList = getOfferIdList()
    splitedList = list()
    selector = {"type" : "selector", "dimension" : "offer_id", "value" : ""}
    for i in range(0, len(offerList), OFFER_SPLIT_OFFSET):
        internalList = list()
        internaloffers = offerList[i:i+OFFER_SPLIT_OFFSET]
        for offer in internaloffers:
            selector["value"] = offer
            internalList.append(selector)
        splitedList.append(internalList)
    return splitedList


def getResponse(param, url = QUERYURL):
    geturl = (url + param) % getTimestamp()
    try:
        r = requests.get(geturl)
        if r.status_code == 200:
            return r.text
        else:
            return None
    except Exception as ex:
        traceback.print_exc()

def getResult(param, url = QUERYURL):
    geturl = url + param
    try:
        r = requests.get(geturl)
        if r.status_code == 200:
            return r.text
        else:
            return None
    except Exception as ex:
        traceback.print_exc()

def getLogger(loggername):
    logging.config.fileConfig(os.path.join(os.path.split(os.path.abspath(sys.path[0]))[0], 'conf/loggerconf.ini'))
    logger = logging.getLogger(loggername)
    return logger

def getBrokerQueryResult():
    s = Template(REALTIME_QUERY_TEMPLATE)
    datasourceTable = dict()
    for ds in DATASOURCE_LIST:
        queryParam = REALTIME_QUERY_TEMPLATE % (ds, getDate()[0], getDate()[1])
        r = requests.post(BROKER_URL, data=queryParam)
        data = json.loads(r.text)[0]
        try:
            key = ds.replace('.', '_')
            datasourceTable[key] = data.get('result').get('click'), data.get('result').get('conversion')
        except Exception as ex:
            traceback.print_exc()
    return datasourceTable

def hasRedundantConv(dataSet):
    for data in dataSet:
        if data[1] != 0:
            return "Exist:{0}".format(data)
    return "NotExist"

def hashourDataEqualsZero(dataSet):
    for data in dataSet:
        if data[2] == 0 or data[3] == 0:
            return "Exist:{0}".format(data)
    return "NotExist"

def get_jsondata(result):
    try:
        return json.loads(result)
    except Exception as ex:
        traceback.print_exc()
    return None

def getSumMetricMap(result):
    jsonResult = get_jsondata(result).get('data').get('data')
    key = jsonResult[0]
    value = jsonResult[1:]
    if key != METRIC:
        metric_map = copy.deepcopy(METIRC_TABLE) # fix bug metric_map = METIRC_TABLE -> metric_map = copy.deepcopy(METIRC_TABLE)
        for v in value:
            data = dict(zip(key, v))
            for metrickey in data:
                if metrickey in METRIC:
                    metric_map[metrickey] += data.get(metrickey)
                else:
                    pass
        for key in metric_map:
            if key in METRICMAP:
                precision = METRICMAP.get(key).get('precision')
                if precision > 0:
                    metric_map[key] = round(metric_map.get(key), precision)
                else:
                    metric_map[key] = int(metric_map.get(key))
        return metric_map
    else:
        return dict(zip(key, value[0]))

def get_affid():
    result = getResponse(AFFID_PARAM)
    affidList = list()
    try:
        data = json.loads(result).get('data').get('data')
        for d in data[1:]:
            affid = d[0]
            if affid not in ["-1", "1"]:
                affidList.append(affid)
    except Exception as ex:
        traceback.print_exc()
    return affidList

def getSortedMap(result):
    metricMap = getSumMetricMap(result)
    keys = metricMap.keys()
    keys.sort()
    values = [metricMap[key] for key in keys]
    return json.dumps(dict(zip(keys, values)))


def getHtmlContent(collectorList, queryList, impalaList, brokerList, querycountlist):
    t = Template(RESULTTEMPLATE)
    s = RESULTMAP
    try:
        s['realtime_slave0_ip'] = collectorList[0].get('ip')
        s['slave0_big'] = collectorList[0].get('ymds_druid_datasource')
        s['slave0_six'] = collectorList[0].get('ymds_druid_datasource6')
        s['realtime_slave1_ip'] = collectorList[1].get('ip')
        s['slave1_big'] = collectorList[1].get('ymds_druid_datasource')
        s['slave1_six'] = collectorList[1].get('ymds_druid_datasource6')
        s['realtime_slave2_ip'] = collectorList[2].get('ip')
        s['slave2_big'] = collectorList[2].get('ymds_druid_datasource')
        s['slave2_six'] = collectorList[2].get('ymds_druid_datasource6')
        s['sum_datasource'] = s.get('slave0_big') + s.get('slave1_big') + s.get('slave2_big')
        s['sum_datasource_six'] = s.get('slave0_six') + s.get('slave1_six') + s.get('slave2_six')
    except Exception as ex:
        traceback.print_exc()

    queryResult = queryList[0]
    try:
        s['ymredundantconv'] = hasRedundantConv(json.loads(queryResult.get('YMREDUNDANTCONV')).get('data').get('data')[1:])
        s['ymdata'] = json.loads(queryResult.get('YMCLICK')).get('data').get('data')[1][0], json.loads(queryResult.get('YMCONV')).get('data').get('data')[1][0]
        s['ymperhourdata'] = hashourDataEqualsZero(json.loads(queryResult.get('YMPERHOURDATA')).get('data').get('data')[1:])
        s['ymunauthcountry'] = json.loads(queryResult.get('YMUNAUTHCOUNTRY')).get('data').get('data')[1][0]
    except Exception as ex:
        traceback.print_exc()


    try:
        impalaResult = impalaList[0]
        s['impaladata'] = impalaResult.get('impalaClick', 0), impalaResult.get('impalaConv', 0)
    except Exception as ex:
        s['impaladata'] = -1, -1

    try:
        brokerResult = brokerList[0]
        s['broker_datasource'] = brokerResult.get('ymds_druid_datasource')
        s['broker_datasource_six'] = brokerResult.get('ymds_druid_datasource_6')
    except Exception as ex:
        traceback.print_exc()

    try:
        sumcount = 0
        for querycountResult in querycountlist:
            sumcount += querycountResult.get('querycount')
        s['querycount'] = sumcount
    except Exception as ex:
        traceback.print_exc()

    try:
        return t.substitute(s)
    except UnicodeError as ex:
        traceback.print_exc()

def getEmailTitle():
    return "daily statistics {0}".format(getDate()[0])

class Emailer(object):

    def __init__(self):
        pass

    def connect(self):
        self.server = smtplib.SMTP(SMTPSERVER, SMTPPORT)
        self.server.ehlo()
        self.server.login(SMTPUSER, SMTPPASSWORD)

    def sendMessage(self, title, content):
        self.connect()
        msg = MIMEText(content, _subtype='html', _charset='utf-8')
        msg['Subject'] = title
        msg['To'] = ','.join(TOLIST)
        try:
            failed = self.server.sendmail(SMTPUSER, TOLIST, msg.as_string())  # may also raise exc
        except Exception as ex:
            traceback.print_exc()
        else:
            return SEND_EMAIL_SUCCESS

class DatabaseInterface(object):

    def __init__(self, host = MONGODB_IP, port = MONGODB_PORT):
        self.host = host
        self.port = port
        self.connectionStatus = self.setConn()

    def setConn(self):
        try:
            conn = pymongo.Connection(self.host, self.port)
        except Exception as e:
            traceback.print_exc()
        else:
            self.monitor = conn.monitordb.monitor
        return 1

    def insertCollection(self, dataSet):
        if self.connectionStatus:
            self.monitor.insert(dataSet)


    def getQueryResult(self):
        if self.connectionStatus:
            return self.monitor.find()
        else:
            return None

    def deleteRecord(self):
        if self.connectionStatus:
            self.monitor.remove({})

class Equaler(object):

    def __init__(self, result):
        self.result = json.loads(result)

    def __eq__(self, other):
        keys = self.result.keys()
        for key in keys:
            if abs(self.result.get(key) - other.result.get(key)) > COMPARE_THRESHOLD:
                return False
        return True

if __name__ == '__main__':
    print getSplitOffers()