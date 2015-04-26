#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

import datetime
import json
import string
import requests
from CountMan.monitor.setting import DUPLICATE_CONV_TMPLATE, BROKER_URL, DUPLICATE_CONV_TID_TMPLATE
from CountMan.monitor.util import getLogger, Emailer

class DuplicateConvMonitor(object):

    def __init__(self):
        self.format = '%Y-%m-%dT%H:00:00'
        self.emiler = Emailer()
        begin = datetime.datetime.today()
        self.logger = getLogger('convmonitor')
        self.beginhour = begin.strftime('%Y-%m-%dT00:00:00')
        self.get_end()
        self.get_param()

    def get_end(self):
        end = datetime.datetime.now()
        self.endhour = end.strftime(self.format)

    def get_param(self):
        self.tid_param = DUPLICATE_CONV_TMPLATE % (self.beginhour, self.endhour)
        self.tid_convtime_param = DUPLICATE_CONV_TID_TMPLATE % (self.beginhour, self.endhour)
        self.logger.info('get broker transaction_id param:{0}'.format(self.tid_param))
        self.logger.info('get broker transaction_id + conv_time param:{0}'.format(self.tid_convtime_param))

    @property
    def monitor(self):
        rlist = list()
        r1 = requests.post(BROKER_URL, data=self.tid_param)
        r2 = requests.post(BROKER_URL, data=self.tid_convtime_param)
        rlist.append(r1)
        rlist.append(r2)
        for r in rlist:
            if r.text == '[ ]':
                self.logger.info('no duplicate conv found in {0} - {1}'.format(self.beginhour, self.endhour))
            else:
                title = 'Warn: found duplicate conv from {0} to {1}'.format(self.beginhour, self.endhour)
                content = r.text
                status = self.emiler.sendMessage(title, content)
                if status:
                    self.logger.info('send email success!')
                else:
                    self.logger.info('send email failed!')

if __name__ == '__main__':
    d = DuplicateConvMonitor()
    d.monitor