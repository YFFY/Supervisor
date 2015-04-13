#! /usr/bin/env python
#! --*-- coding:utf-8 --*--


# This script is used to zip realtime file

import os
import sys
sys.path.append(os.path.split(os.path.split(os.path.abspath(sys.path[0]))[0])[0])

from CountMan.monitor.setting import REALTIMEDIR
import requests
import time

def setTarFile():
    absFileList = [os.path.join(REALTIMEDIR, fileName) for fileName in os.listdir(REALTIMEDIR)]
    tarCommand = "tar -zcvf {0}.tar.gz {1}"
    for absFile in absFileList:
        status = os.popen(tarCommand.format(absFile, absFile)).read()
        print status

def sendClick(maxCount):
    for i in range(maxCount):
        r = requests.get('http://172.30.10.146:8080/trace?offer_id=100131&aff_id=90010417&aff_sub=affsub&aff_sub2=affsub2&aff_sub3=affsub3&aff_sub4=affsub4&aff_sub5=affsub5&aff_sub6=affsub6&aff_sub7=affsub7&aff_sub8=affsub8')
        print r.status_code
        time.sleep(0.1)

if __name__ == "__main__":
    setTarFile()