#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

import datetime
import time
import json
import string
import requests
from CountMan.monitor.setting import DUPLICATE_CONV_TMPLATE, BROKER_URL, DUPLICATE_CONV_TID_TMPLATE
from CountMan.monitor.util import getLogger, Emailer, getSplitOffers

class DuplicateConvMonitor(object):

    def __init__(self):
        self.format = '%Y-%m-%dT%H:00:00'
        self.emiler = Emailer()
        self.logger = getLogger('convmonitor')
        self.splitoffers = getSplitOffers()
        self.get_trange()
        # self.get_param()

    def get_trange(self):
        end = datetime.datetime.now()
        if end.hour == 0:
            begin = datetime.datetime.today() + datetime.timedelta(days=-1)
        else:
            begin = datetime.datetime.today()
        self.beginhour = begin.strftime('%Y-%m-%dT00:00:00')
        self.endhour = end.strftime(self.format)

    def get_param(self):
        self.tid_param = DUPLICATE_CONV_TMPLATE % (self.beginhour, self.endhour)
        self.tid_convtime_param = DUPLICATE_CONV_TID_TMPLATE % (self.beginhour, self.endhour)

    def send(self, broker_param, title):
        try:
            r = requests.post(BROKER_URL, data = broker_param)
            if r.text == '[ ]':
                self.logger.info('not find duplicate conversions between {0} to {1}, get {2}'.format(self.beginhour, self.endhour, r.text))
            else:
                self.logger.error('find duplicate conversions between {0} to {1}, get {2}'.format(self.beginhour, self.endhour, r.text))
                try:
                    count = len(json.loads(r.text))
                except Exception as ex:
                    count = "many"
                status = self.emiler.sendMessage(title.format(count, self.beginhour, self.endhour), r.text)
                if status:
                    self.logger.info('send email success!')
                else:
                    self.logger.info('send email failed!')
        except Exception as ex:
            self.logger.error('error occuar when send http post to broker')

    @property
    def monitor(self):
        for selectorList in self.splitoffers:
            selectorStr = str(selectorList).replace("'", '"')
            tid_param = DUPLICATE_CONV_TMPLATE % (self.beginhour, self.endhour, selectorStr)
            tid_convtime_param = DUPLICATE_CONV_TID_TMPLATE % (self.beginhour, self.endhour, selectorStr)
            self.send(tid_param, 'use: [transaction_id] find {0} duplicate conversions between {1} to {2}')
            self.send(tid_convtime_param, 'use: [transaction_id, conv_time] find {0} duplicate conversions between {1} to {2}')

if __name__ == '__main__':
    d = DuplicateConvMonitor()
    d.monitor