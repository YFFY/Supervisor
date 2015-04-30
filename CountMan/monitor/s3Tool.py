#! /usr/bin/env python
# --*-- coding:utf-8 --*--

import os
import sys
from datetime import datetime, timedelta



class Uploader(object):

    def __init__(self):
        self.timeformat = '%Y-%m-%dT%H'
        self.bucket = 's3://druid.s3.internal_stable.Storage/druidbatch.store/'
        self.druidBatchDir = '/data2/druidBatchData'
        self.getSuffix()

    def getSuffix(self):
        current = datetime.now()
        end = current + timedelta(hours=-1)
        start = current + timedelta(hours=-2)
        self.fileSuffix =  '{0}_{1}_yfnormalpf.json'.format(
            start.strftime(self.timeformat), end.strftime(self.timeformat)
        )

    def getFile(self):
        for f in os.listdir(self.druidBatchDir):
            if f.endswith(self.fileSuffix):
                return f, os.path.join(self.druidBatchDir, f)
        return None

    def makeCompression(self):
        try:
            self.fileName, self.absFile = self.getFile()
        except TypeError as ex:
            logging.error('can not find any file')
        else:
            self.compressfile = '{0}.tar.gz'.format(self.fileName)
            tarCmd = 'tar -zcf {0} {1}'.format(self.compressfile, self.absFile)
            status = os.system(tarCmd)
            if status != 0:
                sys.exit()

    @property
    def upload(self):
        self.makeCompression()
        self.s3path = '{0}{1}'.format(self.bucket, self.compressfile)
        uploadCmd = 's3cmd put {0} {1}'.format(
            self.compressfile, self.s3path
        )
        status = os.system(uploadCmd)
        if status == 0:
            if self.checkSuccess():
                self.deleteLocalFile()
                print 'get file: {0}, upload to s3 success'.format(self.fileName)
            else:
                print 'get file: {0}, upload to s3 failed'.format(self.fileName)
        else:
            print 'execute upload command: {0} failed'.format(uploadCmd)

    def checkSuccess(self):

        localFileSize = os.path.getsize(self.compressfile)
        infoCmd = 's3cmd info {0}'.format(self.s3path)
        content = os.popen(infoCmd).readlines()
        s3FileSize = int(content[1].split(':')[1].strip())
        if localFileSize == s3FileSize:
            return True
        return False

    def deleteLocalFile(self):
        os.remove(self.compressfile)

if __name__ == '__main__':
    u = Uploader()
    u.upload
