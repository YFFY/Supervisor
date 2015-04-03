#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import traceback
from CountMan.setting import IMPALA_IP
from CountMan.util import getDate, DatabaseInterface

def getImpalaData():
    impalaData = dict()
    day = getDate()[0]
    try:
        clickCommand = """impala-shell -i {0} -q 'use ym_system;select count(1) from ym_impala where part1="{1}" and log_tye =0;'""".format(IMPALA_IP, day)
        convCommand = """impala-shell -i {0} -q 'use ym_system;select count(1) from ym_impala where part1="{1}" and log_tye =1 and datasource!="hasoffer" and status="Confirmed";'""".format(IMPALA_IP, day)
        impalaData['impalaClick'] = os.popen(clickCommand).read().split()[-3]
        impalaData['impalaConv'] = os.popen(convCommand).read().split()[-3]
        return impalaData
    except Exception as ex:
        # traceback.print_exc()
        pass
    return {"impalaClick": 0, "impalaConv": 0}

def set2db():
    dao = DatabaseInterface()
    dao.insertCollection(getImpalaData())

if __name__ == '__main__':
    set2db()