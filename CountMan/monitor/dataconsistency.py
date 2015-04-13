#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.abspath(sys.path[0]))[0])


from CountMan.monitor.util import getTimestamp, getResult, getSortedMap, Equaler, getLogger, Emailer
from CountMan.monitor.setting import *
from itertools import combinations


class Monitor(object):

    def __init__(self):
        self.logger = getLogger('consistency')
        self.emailer = Emailer()

    def get_group(self):
        self.groupList = list()
        for itercount in range(1, len(DIMENSION)):
            for combination in combinations(DIMENSION, itercount):
                self.groupList.append(combination)
        self.logger.info('get group combination count: {0}'.format(len(self.groupList)))

    def get_result(self):
        self.get_group()
        start, end = getTimestamp()
        no_dimension_param = NO_DIMENSION_PARAM % (start, end, METRIC)
        no_dimension_result = getResult(no_dimension_param.replace("'", '"'))
        no_dimension_map = getSortedMap(no_dimension_result)
        for group in self.groupList:
            dimension_param = DIMENSION_PARAM % (start, end, METRIC, list(group))
            dimension_result = getResult(dimension_param.replace("'", '"'))
            dimesion_map = getSortedMap(dimension_result)
            if not dimesion_map or not no_dimension_map:
                self.logger.info('dimension query result: {0}\ntimeseries query result: {1}\n'.format(dimesion_map, no_dimension_map))
                return -1
            if Equaler(dimesion_map) == Equaler(no_dimension_map):
                isPass = True
            else:
                isPass = False
            if isPass:
                self.logger.info("{0}\t{1}\n".format(isPass, group))
            else:
                title = 'Warn : Detected data inconsistencies with group : {0}'.format(group)
                content = """group: {0}
                          dimension query result: {1}
                          timeseries query result: {2}""".format(group, dimesion_map, no_dimension_map)
                self.logger.error(content)
                status = self.emailer.sendMessage(title, content)
                if status:
                    self.logger.info('send email success')
                else:
                    self.logger.error('send email failed')


if __name__ == '__main__':
    m = Monitor()
    m.get_result()