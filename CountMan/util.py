#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import time
import requests
import pymongo
import datetime
import json
import logging
from string import Template
import socket
import traceback
import smtplib, email, sys
from CountMan.setting import *
from errcode import *
from email.mime.text import MIMEText

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='../log/counter.log',
                    filemode='a')


def getLocalIp():
    return socket.gethostbyname(socket.gethostname()).replace('.', '-')

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

def setLog(message):
    logging.debug(message)

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

def getHtmlContent(collectorList, queryList, impalaList):
    t = Template(RESULTTEMPLATE)
    s = RESULTMAP
    try:
        s['realtime_slave0_ip'] = collectorList[0].get('ip')
        s['slave0_big'] = collectorList[0].get('ymds_druid_datasource')
        s['slave0_zero'] = collectorList[0].get('ymds_druid_datasource0')
        s['slave0_four'] = collectorList[0].get('ymds_druid_datasource4')
        s['slave0_six'] = collectorList[0].get('ymds_druid_datasource6')
        s['realtime_slave1_ip'] = collectorList[1].get('ip')
        s['slave1_big'] = collectorList[1].get('ymds_druid_datasource')
        s['slave1_zero'] = collectorList[1].get('ymds_druid_datasource0')
        s['slave1_four'] = collectorList[1].get('ymds_druid_datasource4')
        s['slave1_six'] = collectorList[1].get('ymds_druid_datasource6')
        s['realtime_slave2_ip'] = collectorList[2].get('ip')
        s['slave2_big'] = collectorList[2].get('ymds_druid_datasource')
        s['slave2_zero'] = collectorList[2].get('ymds_druid_datasource0')
        s['slave2_four'] = collectorList[2].get('ymds_druid_datasource4')
        s['slave2_six'] = collectorList[2].get('ymds_druid_datasource6')
        s['realtime_slave0_total'] = s.get('slave0_big') + s.get('slave0_zero') + s.get('slave0_four') + s.get('slave0_six')
        s['realtime_slave1_total'] = s.get('slave1_big') + s.get('slave1_zero') + s.get('slave1_four') + s.get('slave1_six')
        s['realtime_slave2_total'] = s.get('slave2_big') + s.get('slave2_zero') + s.get('slave2_four') + s.get('slave2_six')
    except Exception as ex:
        traceback.print_exc()

    queryResult = queryList[0]
    try:
        s['ymduplicateconv'] = json.loads(queryResult.get('YMDUPLICATECONV')).get('data').get('page').get('total')
        s['ymredundantconv'] = hasRedundantConv(json.loads(queryResult.get('YMREDUNDANTCONV')).get('data').get('data')[1:])
        s['ymdata'] = json.loads(queryResult.get('YMCLICK')).get('data').get('data')[1][0], json.loads(queryResult.get('YMCONV')).get('data').get('data')[1][0]
        s['ymperhourdata'] = hashourDataEqualsZero(json.loads(queryResult.get('YMPERHOURDATA')).get('data').get('data')[1:])
        s['ymunauthcountry'] = json.loads(queryResult.get('YMUNAUTHCOUNTRY')).get('data').get('data')[1][0]
    except Exception as ex:
        traceback.print_exc()

    impalaResult = impalaList[0]
    try:
        s['impaladata'] = impalaResult.get('impalaClick', 0), impalaResult.get('impalaConv', 0)
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