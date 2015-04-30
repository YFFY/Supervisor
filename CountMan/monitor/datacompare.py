#! /usr/bin/env python
# --*-- coding:utf-8 --*--

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
        clickSql = """hive -e 'use ym_system;select count(1) from ym_hive where part1='{1}' and log_type=0 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        convSql = """hive -e 'use ym_system;select count(1) from ym_hive where part1='{1}' and log_type=1 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        print 'get hive click sql: {0}'.format(clickSql)
        print 'get hive conv sql: {0}'.format(convSql)
        return int(os.popen(clickSql).read().split()[-3]), int(os.popen(convSql).read().split()[-3])


    def getImpalaResult(self):
        clickSql = """impala-shell -i {0} -q 'use ym_system;select count(1) from ym_impala where part1="{1}" and log_tye =0 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        convSql = """impala-shell -i {0} -q 'use ym_system;select count(1) from ym_impala where part1="{1}" and log_tye =1 and hour(time_stamp)={2};'""".format(
            IMPALA_IP, self.today, self.hour
        )
        print 'get impala click sql: {0}'.format(clickSql)
        print 'get impala conv sql: {0}'.format(convSql)
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
        equal = self.isEqual()
        if not equal:
            title = 'data not equal between druid and hive and impala at {0} {1}'.format(self.today, self.hour)
            content = 'druid: {0}\nhive: {1}\nimpala: {2}'.format(equal[0], equal[1], equal[2])
            self.emailer.sendMessage(title, content)
        else:
            print equal

if __name__ == '__main__':
    dc = DataComparer()
    dc.isEqual()