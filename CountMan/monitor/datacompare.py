#! /usr/bin/env python
# --*-- coding:utf-8 --*--




import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

from util import *

class DataComparer(object):

    def __init__(self):
        self.emailer = Emailer()
        self.getDruidTimeRange()
        self.getHiveImpalaTimeRange()

    def getDruidTimeRange(self):
        self.unixstart, self.unixend = getHourTimestamp()

    def getHiveImpalaTimeRange(self):
        self.today, self.hour = getHourRange()

    def getDruidResult(self):
        self.getDruidTimeRange()
        param = '{"settings":{"time":{"start":%d,"end":%d,"timezone":0},"report_id":"1321231321","data_source":"ymds_druid_datasource","pagination":{"size":1000000,"page":0}},"group":[],"data":["click","conversion"],"filters":{"$and":{}},"sort":[]}' % (self.unixstart, self.unixend)
        try:
            r = requests.get(QUERYURL + param)
            data = json.loads(r.text).get('data').get('data')
            return data[1][0], data[1][1]
        except Exception as ex:
            raise ex
        else:
            return 0, 0

    def getHiveResult(self):
        clickSql = """hive -e 'use ym_system;select count(distinct(transaction_id)) from ym_hive where part1="{1}" and log_type=0 and hour(regexp_replace(time_stamp,"T"," "))={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        convSql = """hive -e 'use ym_system;select count(distinct(transaction_id)) from ym_hive where part1="{1}" and log_type=1 and hour(regexp_replace(time_stamp,"T"," "))={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        return int(os.popen(clickSql).readlines()[-1].strip()), int(os.popen(convSql).readlines()[-1].strip())


    def getImpalaResult(self):
        clickSql = """impala-shell -i {0} -q 'use ym_system;select count(distinct(transaction_id)) from ym_impala where part1="{1}" and log_tye =0 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        convSql = """impala-shell -i {0} -q 'use ym_system;select count(distinct(transaction_id)) from ym_impala where part1="{1}" and log_tye =1 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        return int(os.popen(clickSql).read().split()[-3]), int(os.popen(convSql).read().split()[-3])

    def isEqual(self):
        druidResult = self.getDruidResult()
        impalaResult = self.getImpalaResult()
        if druidResult == impalaResult:
            return True
        else:
            hiveResult = self.getHiveResult()
        return druidResult, hiveResult, impalaResult

    def sendEmail(self):
        result = self.isEqual()
        if not isinstance(result, tuple):
            title = 'data not equal between druid and hive and impala at {0} {1}'.format(self.today, self.hour)
            content = 'druid: {0}\nhive: {1}\nimpala: {2}'.format(equal[0], equal[1], equal[2])
            self.emailer.sendMessage(title, content)
        else:
            print equal

if __name__ == '__main__':
    dc = DataComparer()
    dc.sendEmail()