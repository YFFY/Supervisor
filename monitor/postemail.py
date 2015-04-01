#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import pymongo
import json
import datetime
import smtplib, email, sys
from email.mime.text import MIMEText


def getDate():
    t = datetime.datetime.today()
    return t.strftime('%Y-%m-%d')


def getdbdata():
    try:
        conn = pymongo.Connection('10.1.5.60', 27017)
    except Exception as e:
        return None
    else:
        cursor = conn.monitordb.monitor

    find = cursor.find()
    day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, yeahmobiRepeatConv, unauthcountrynum, nativeclickconv = 14 * [0]
    hasredundantConv = False
    missHour = False
    for data in find:
        del data['_id']
        if data.get('ip') == '10-1-15-13':
            collector_13 = data.get('collector')
            batch_13 = data.get('batch')
            hadoop_13 = data.get('hadoop')
            day = data.get('datetime')
        if data.get('ip') == '10-1-15-12':
            collector_12 = data.get('collector')
            batch_12 = data.get('batch')
            hadoop_12 = data.get('hadoop')
        if 'yeahmobiClick' and 'yeahmobiConv' in data:
            ymclick = json.loads(data.get('yeahmobiClick')).get('data').get('data')[1][0]
            ymconv = json.loads(data.get('yeahmobiConv')).get('data').get('data')[1][0]
            ymtotal = [ymclick, ymconv]
        if 'yeahmobiRepeatConv' in data:
            try:
                yeahmobiRepeatConv = len(json.loads(data.get('yeahmobiRepeatConv')).get('data').get('data')) - 1
            except ValueError as e:
                raise e
        if 'hasredundantConv' in data:
            try:
                hasredundantConvData = json.loads(data.get('hasredundantConv')).get('data').get('data')
                for convdata in hasredundantConvData[1:]:
                    if convdata[-1] > 0:
                        hasredundantConv = True
                    else:
                        pass
            except ValueError as e:
                raise e
        if 'hourData' in data:
            dataSet = json.loads(data.get('hourData')).get('data').get('data')[1:]
            for hdata in dataSet:
                if hdata[2] == 0 or hdata[3] == 0:
                    missHour += 'UTC: {0}-{1}\n'.format(hdata[0], hdata[1])
        if 'unauthcountry' in data:
            unauthcountrynum = json.loads(data.get('unauthcountry')).get('data').get('data')[1][0]
        if 'nativeClickConv' in data:
            nativeclickconv = json.loads(data.get('nativeClickConv')).get('data').get('data')[1][0]
    total_collector = collector_12 + collector_13
    total_batch = batch_12 + batch_13
    total_hadoop = hadoop_12 + hadoop_13
    cursor.remove()
    return day, collector_12, collector_13, total_collector, batch_12, batch_13, total_batch, hadoop_12, hadoop_13, total_hadoop, ymtotal, yeahmobiRepeatConv, hasredundantConv, missHour,unauthcountrynum, nativeclickconv

def getTemplate():
    with open('template', 'r') as f:
        return f.read()

def getHtmlContent():

    dbdata = getdbdata()
    if not dbdata:
        return None
    dataSet = dict(zip(
        ["day", "collector_12", "collector_13", "total_collector", "batch_12", "batch_13", "total_batch", "hadoop_12",
         "hadoop_13", "total_hadoop", "ymtotal", "yeahmobiRepeatConv", "hasredundantConvData",
         "missHour","unauthcountrynum", "nativeclickconv"],
        dbdata))
    html_template = getTemplate()
    return html_template.format(dataSet.get('day'),
                                dataSet.get('collector_12'),
                                dataSet.get('collector_13'),
                                dataSet.get('total_collector'),
                                dataSet.get('batch_12'),
                                dataSet.get('batch_13'),
                                dataSet.get('total_batch'),
                                dataSet.get('hadoop_12'),
                                dataSet.get('hadoop_13'),
                                dataSet.get('total_hadoop'),
                                dataSet.get('ymtotal'),
                                dataSet.get('yeahmobiRepeatConv'),
                                convert(dataSet.get('hasredundantConv')),
                                convert(dataSet.get('missHour')),
                                dataSet.get('unauthcountrynum'),
                                dataSet.get('nativeclickconv'))


smtpserver = 'smtp.163.com'
smtpuser = '15251826346@163.com'
smtppass = 'bmeB500!'
smtpport = '25'


def convert(boolean):
    if boolean:
        return "Exist"
    else:
        return "Not Exist"


def connect():
    server = smtplib.SMTP(smtpserver, smtpport)
    server.ehlo()
    server.login(smtpuser, smtppass)
    return server


def sendmessage(server, to, subj, content):
    if not content:
        return -1
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = subj
    msg['To'] = ','.join(to)
    try:
        failed = server.sendmail(smtpuser, to, msg.as_string())  # may also raise exc
    except Exception as ex:
        print 'Error - send failed'
    else:
        print '{0}: send success'.format(getDate())


if __name__ == "__main__":
    #toList = ['bigdata@ndpmedia.com','robin.hu@ndpmedia.com', 'jeff.yu@ndpmedia.com', 'hardy.tan@ndpmedia.com']
    toList = ['jeff.yu@ndpmedia.com']
    subj = 'daily statistics at {0}'.format(getDate())
    text = getHtmlContent()
    server = connect()
    sendmessage(server, toList, subj, text)
