#! /usr/bin/env python
# --*-- coding:utf-8 --*--

ISDEBUG = False  # imitator.py is only used to debug

REALTIME_NODE_COUNT = 3  # three realtime node

RESULTMAP = {
    "realtime_slave0_ip":"",
    "slave0_big":0,
    "slave0_zero":0,
    "slave0_four":0,
    "slave0_six":0,
    "realtime_slave1_ip":"",
    "slave1_big":0,
    "slave1_zero":0,
    "slave1_four":0,
    "slave1_six":0,
    "realtime_slave2_ip":"",
    "slave2_big":0,
    "slave2_zero":0,
    "slave2_four":0,
    "slave2_six":0,
    "realtime_slave0_total":0,
    "realtime_slave1_total":0,
    "realtime_slave2_total":0,
    "ymduplicateconv":0,
    "ymredundantconv":0,
    "ymdata":0,
    "ymperhourdata":0,
    "ymunauthcountry":0,
    "impaladata":0
}


# email template
RESULTTEMPLATE = """
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title></title>
</head>
<body>
<table border="1">
<tr>
  <td></td>
  <td>datasource</td>
  <td>datasource.0</td>
  <td>datasource.4</td>
  <td>datasource.6</td>
  <td>total</td>
</tr>
<tr>
  <td>$realtime_slave0_ip</td>
  <td>$slave0_big</td>
  <td>$slave0_zero</td>
  <td>$slave0_four</td>
  <td>$slave0_six</td>
  <td>$realtime_slave0_total</td>
</tr>
<tr>
  <td>$realtime_slave1_ip</td>
  <td>$slave0_big</td>
  <td>$slave0_zero</td>
  <td>$slave0_four</td>
  <td>$slave0_six</td>
  <td>$realtime_slave1_total</td>
</tr>
<tr>
  <td>$realtime_slave2_ip</td>
  <td>$slave0_big</td>
  <td>$slave0_zero</td>
  <td>$slave0_four</td>
  <td>$slave0_six</td>
  <td>$realtime_slave2_total</td>
</tr>
</table>
<br>
<table border="1">
<tr>
  <td>Query Content</td>
  <td>Query Result</td>
</tr>
<tr>
  <td>YeahMobi Duplicate Conv</td>
  <td>$ymduplicateconv</td>
</tr>
<tr>
  <td>status!=Confirmed&conversion=1</td>
  <td>$ymredundantconv</td>
</tr>
<tr>
  <td>YeahMobi Click and Conv</td>
  <td>$ymdata</td>
</tr>
<tr>
  <td>YeahMobi Per Hour Click and Conv</td>
  <td>$ymperhourdata</td>
</tr>
<tr>
  <td>YeahMobi Unauth Country</td>
  <td>$ymunauthcountry</td>
</tr>
<tr>
  <td>Impala Data</td>
  <td>$impaladata</td>
</tr>
</table>
</body>
</html>
"""


QUERYURL = "http://resin-yeahmobi-214401877.us-east-1.elb.amazonaws.com:18080/report/report?report_param="

# query param
QUERYPARAM = {
    "YMDUPLICATECONV" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["transaction_id"],"data":["click","conversion"],"filters":{"$and":{"datasource":{"$neq":"hasoffer"},"status":{"$eq":"Confirmed"},"log_tye":{"$eq":1},"conversion":{"$gt":1}}},"sort":[]}',
    "YMREDUNDANTCONV" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"data_source":"ymds_druid_datasource","report_id":"convesionLogQuery","pagination":{"size":50,"page":0}},"data":["conversion"],"group":["status"],"filters":{"$and":{"log_tye":{"$eq":"1"},"status":{"$neq":"Confirmed"}}}}',
    "YMCLICK" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":[],"data":["click"],"filters":{"$and":{}},"sort":[]}',
    "YMCONV" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":[],"data":["conversion"],"filters":{"$and":{"log_tye":{"$eq":"1"},"status":{"$eq":"Confirmed"},"datasource":{"$neq":"hasoffer"}}},"sort":[]}',
    "YMPERHOURDATA" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":["day","hour"],"data":["click","conversion"],"filters":{"$and":{}},"sort":[]}',
    "YMUNAUTHCOUNTRY" : '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":[],"data":["conversion2"],"filters":{"$and":{"datasource":{"$neq":"hasoffer"},"log_tye":{"$eq":1},"status":{"$eq":"Rejected"},"message":{"$eq":"unauthenticated country"}}},"sort":[]}'
}

if ISDEBUG:
    REALTIMEDIR = "/data/tmpdata/druid_consumer_log"
    MONGODB_IP = "172.20.1.184"
    STATISTICSDAY = 1 # today
    IMPALA_IP = "salve-72"
else:
    REALTIMEDIR = "/tmpdata/druid_consumer_log"
    MONGODB_IP = "10.1.5.60"
    STATISTICSDAY = 0 # yesterday
    IMPALA_IP = "ip-10-1-33-22.ec2.internal"


MONGODB_PORT = 27017
TOLIST = ['jeff.yu@ndpmedia.com']
#TOLIST = ['bigdata@ndpmedia.com','robin.hu@ndpmedia.com', 'jeff.yu@ndpmedia.com', 'hardy.tan@ndpmedia.com']
SMTPSERVER = 'smtp.163.com'
SMTPPORT = '25'
SMTPUSER = '15251826346@163.com'
SMTPPASSWORD = 'bmeB500!'