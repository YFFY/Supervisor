#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])


from CountMan.monitor.util import getHourTimestamp, getResult, getSortedMap, Equaler, getLogger, Emailer, get_affid
from CountMan.monitor.setting import *
from itertools import combinations


class Monitor(object):

    def __init__(self):
        self.logger = getLogger('consistency')
        self.emailer = Emailer()
        self.affidList = get_affid()

    def get_group(self):
        self.groupList = list()
        for itercount in range(1, len(DIMENSION)):
            for combination in combinations(DIMENSION, itercount):
                self.groupList.append(combination)
        self.logger.info('get group combination count: {0}'.format(len(self.groupList)))

    def get_result(self):
        self.get_group()
        start, end = getHourTimestamp()
        aff_id = choice(self.affidList)
        no_dimension_param = NO_DIMENSION_PARAM % (start, end, METRIC, aff_id)
        no_dimension_result = getResult(no_dimension_param.replace("'", '"'))
        no_dimension_map = getSortedMap(no_dimension_result)
        for group in self.groupList:
            dimension_param = DIMENSION_PARAM % (start, end, METRIC, list(group), aff_id)
            dimension_result = getResult(dimension_param.replace("'", '"'))
            dimesion_map = getSortedMap(dimension_result)
            if not dimesion_map or not no_dimension_map:
                self.logger.info('result is not exist: dimension query result: {0}\ntimeseries query result: {1}\n'.format(dimesion_map, no_dimension_map))
                return -1
            if Equaler(dimesion_map) == Equaler(no_dimension_map):
                self.logger.info("{0}\t{1}\t{2}\n".format("pass", group, aff_id))
            else:
                title = 'Warn : Detected data inconsistencies with group: {0} and aff_id: {1}'.format(group, aff_id)
                content = """
                          aff_id: {0}
                          group: {1}
                          dimension query result: {2}
                          timeseries query result: {3}""".format(aff_id, group, dimesion_map, no_dimension_map)
                self.logger.error(content)
                status = self.emailer.sendMessage(title, content)
                if status:
                    self.logger.info('send email success')
                else:
                    self.logger.error('send email failed')


if __name__ == '__main__':
    m = Monitor()
    m.get_result()