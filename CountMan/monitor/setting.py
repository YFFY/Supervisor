#! /usr/bin/env python
# --*-- coding:utf-8 --*--

from random import choice

ISDEBUG = False  # imitator.py is only used to debug

LOGGING_LEVEL = "DEBUG"

REALTIME_NODE_COUNT = 3  # three realtime node

DATASOURCE_LIST = "ymds_druid_datasource", "ymds_druid_datasource.0", "ymds_druid_datasource.4", "ymds_druid_datasource.6"

REALTIME_QUERY_TEMPLATE = """{
  "queryType" : "timeseries",
  "dataSource" : {
    "type" : "table",
    "name" : "%s"
  },
  "intervals" : {
    "type" : "LegacySegmentSpec",
    "intervals" : [ "%sT00:00:00.000Z/%sT00:00:00.000Z" ]
  },
  "filter" : null,
  "granularity" : {
    "type" : "all"
  },
  "aggregations" : [ {
    "type" : "longSum",
    "name" : "click",
    "fieldName" : "click"
  }, {
    "type" : "longSum",
    "name" : "conversion",
    "fieldName" : "conversion"
  } ],
  "postAggregations" : [ ],
  "context" : null
}"""

DUPLICATE_CONV_TMPLATE = """{
  "queryType" : "groupBy",
  "dataSource" : {
    "type" : "table",
    "name" : "ymds_druid_datasource"
  },
  "intervals" : {
    "type" : "LegacySegmentSpec",
    "intervals" : [ "%s.000Z/%s.000Z" ]
  },
  "filter" : {
    "type" : "and",
    "fields" : [ {
      "type" : "not",
      "field" : {
        "type" : "selector",
        "dimension" : "datasource",
        "value" : "hasoffer"
      }
    }, {
      "type" : "selector",
      "dimension" : "log_tye",
      "value" : "1"
    }, {
      "type" : "selector",
      "dimension" : "status",
      "value" : "Confirmed"
    } ]
  },
  "granularity" : {
    "type" : "all"
  },
  "dimensions" : [ {
    "type" : "typed",
    "dimension" : "transaction_id",
    "outputName" : "transaction_id",
    "dimType" : "PLAIN"
  } ],
  "aggregations" : [ {
    "type" : "longSum",
    "name" : "conversion",
    "fieldName" : "conversion"
  } ],
  "postAggregations" : [ ],
  "having" : {
    "type" : "and",
    "havingSpecs" : [ {
      "type" : "greaterThan",
      "aggregation" : "conversion",
      "value" : 1
    } ]
  },
  "orderBy" : {
    "type" : "default",
    "columns" : [ ],
    "limit" : 10000
  },
  "context" : null
}"""


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
    "sum_datasource":0,
    "sum_datasource_zero":0,
    "sum_datasource_four":0,
    "sum_datasource_six":0,
    "broker_datasource":0,
    "broker_datasource_zero":0,
    "broker_datasource_four":0,
    "broker_datasource_six":0,
    "ymduplicateconv":0,
    "ymredundantconv":0,
    "ymdata":0,
    "ymperhourdata":0,
    "ymunauthcountry":0,
    "impaladata":0,
    "querycount":0
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
  <td>Consumer Log</td>
  <td>datasource</td>
  <td>datasource.0</td>
  <td>datasource.4</td>
  <td>datasource.6</td>
</tr>
<tr>
  <td>$realtime_slave0_ip</td>
  <td>$slave0_big</td>
  <td>$slave0_zero</td>
  <td>$slave0_four</td>
  <td>$slave0_six</td>
</tr>
<tr>
  <td>$realtime_slave1_ip</td>
  <td>$slave1_big</td>
  <td>$slave1_zero</td>
  <td>$slave1_four</td>
  <td>$slave1_six</td>
</tr>
<tr>
  <td>$realtime_slave2_ip</td>
  <td>$slave2_big</td>
  <td>$slave2_zero</td>
  <td>$slave2_four</td>
  <td>$slave2_six</td>
</tr>
<tr>
  <td>total</td>
  <td>$sum_datasource</td>
  <td>$sum_datasource_zero</td>
  <td>$sum_datasource_four</td>
  <td>$sum_datasource_six</td>
</tr>
</table>
<br>
<table border="1">
<tr>
  <td>Broker Query</td>
  <td>datasource</td>
  <td>datasource.0</td>
  <td>datasource.4</td>
  <td>datasource.6</td>
</tr>
<tr>
  <td></td>
  <td>$broker_datasource</td>
  <td>$broker_datasource_zero</td>
  <td>$broker_datasource_four</td>
  <td>$broker_datasource_six</td>
</tr>
</table>
<br>
<table border="1">
<tr>
  <td>Resin Query</td>
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
  <td>YeahMobi Click = 0 or Conv = 0 in Per Hour</td>
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
<tr>
  <td>Query Count</td>
  <td>$querycount</td>
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
    BROKER_URL = "http://54.210.60.55:8080/druid/v2/?pretty"
    STATISTICSDAY = 1 # today
    IMPALA_IP = "salve-72"
else:
    REALTIMEDIR = "/tmpdata/druid_consumer_log"
    MONGODB_IP = "10.1.5.60"
    BROKER_URL = "http://10.1.5.30:8080/druid/v2/?pretty"
    STATISTICSDAY = 0 # yesterday
    IMPALA_IP = "ip-10-1-33-22.ec2.internal"


MONGODB_PORT = 27017
TOLIST = ['jeff.yu@ndpmedia.com']
#TOLIST = ['bigdata@ndpmedia.com','robin.hu@ndpmedia.com', 'jeff.yu@ndpmedia.com', 'hardy.tan@ndpmedia.com']
SMTPSERVER = 'smtp.163.com'
SMTPPORT = '25'
SMTPUSER = '15251826346@163.com'
SMTPPASSWORD = 'bmeB500!'



DIMENSION = [choice(("aff_id","offer_id")),
             choice(("country",)),
             choice(("platform_id",)),
             choice(("browser",)),
             choice(("day","hour"))]
METRIC = ["click", "conversion", "cost", "revenue", "profit"]

QUERYURL = "http://resin-yeahmobi-214401877.us-east-1.elb.amazonaws.com:18080/report/report?report_param="

METRICMAP = {
  "profit" : {
    "alisa" : "profit",
    "name" : "(revenue-cost)",
    "formula" : "((count(revenue) - count(cost)) as profit)",
    "level" : 2,
    "precision" : 3
  },
  "epc" : {
    "alisa" : "epc",
    "name" : "(cost/click)",
    "formula" : "(count(cost) / count(click) as epc)",
    "level" : 2,
    "precision" : 3
  },
  "conversion" : {
    "alisa" : "conversion",
    "name" : "count(conversion)",
    "formula" : "(count(conversion) as conversion)",
    "level" : 1,
    "precision" : 0
  },
  "arpa" : {
    "alisa" : "arpa",
    "name" : "(revenue/conversion)",
    "formula" : "(count(revenue) / count(conversion) as arpa)",
    "level" : 2,
    "precision" : 2
  },
  "click" : {
    "alisa" : "click",
    "name" : "count(click)",
    "formula" : "(count(click) as click)",
    "level" : 1,
    "precision" : 0
  },
  "acpa" : {
    "alisa" : "acpa",
    "name" : "(cost/conversion)",
    "formula" : "(count(cost) / count(conversion) as acpa)",
    "level" : 2,
    "precision" : 2
  },
  "cost" : {
    "alisa" : "cost",
    "name" : "count(cost)",
    "formula" : "(count(cost) as cost)",
    "level" : 1,
    "precision" : 2
  },
  "cr" : {
    "alisa" : "cr",
    "name" : "(conversion/click)",
    "formula" : "(count(conversion) / count(click) as cr)",
    "level" : 2,
    "precision" : 4
  },
  "rpc" : {
    "alisa" : "rpc",
    "name" : "(revenue/click)",
    "formula" : "(count(revenue) / count(click) as rpc)",
    "level" : 2,
    "precision" : 3
  },
  "cpc" : {
    "alisa" : "cpc",
    "name" : "(cost/click)",
    "formula" : "(count(cost) / count(click) as cpc)",
    "level" : 2,
    "precision" : 3
  },
  "unique_click" : {
    "alisa" : "unique_click",
    "name" : "count(unique_click)",
    "formula" : "(count(unique_click) as unique_click)",
    "level" : 1,
    "precision" : 0
  },
  "revenue" : {
    "alisa" : "revenue",
    "name" : "count(revenue)",
    "formula" : "(count(revenue) as revenue)",
    "level" : 1,
    "precision" : 3
  },
  "rows" : {
    "alisa" : "rows",
    "name" : "count(rows)",
    "formula" : "(count(*) as rows)",
    "level" : 1,
    "precision" : 0
  },
  "conversion2" : {
    "alisa" : "conversion2",
    "name" : "count(conversion2)",
    "formula" : "(count(conversion2) as conversion2)",
    "level" : 1,
    "precision" : 0
  }
}

QUERY_COUNT_PATH = '/mnt/realquery'

METIRC_TABLE = {u'conversion': 0, u'revenue': 0, u'profit': 0, u'cost': 0, u'click': 0}

DIMENSION_PARAM = '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"data_source":"ymds_druid_datasource","report_id":"ForDataConsistency","pagination":{"size":100000,"page":0}},"data":%s,"group":%s,"filters":{"$and":{}}}'
NO_DIMENSION_PARAM = '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"data_source":"ymds_druid_datasource","report_id":"ForDataConsistency","pagination":{"size":100000,"page":0}},"data":%s,"group":[],"filters":{"$and":{}}}'
SUM_METRIC_MAP = {u'conversion': 0, u'revenue': 0.0, u'profit': 0.0, u'cpc': 0.0, u'arpa': 0.00, u'rpc': 0.0, u'cost': 0.0, u'acpa': 0.0, u'cr': 0.0, u'click': 0}

COMPARE_THRESHOLD = 0.05