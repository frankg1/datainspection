#!/usr/local/python/bin
# coding=utf-8
import logging
import logging.handlers
import time
import sys
import os

rq = time.strftime('%Y%m%d',time.localtime(time.time()))

#获取自己这个文件路径全名
filename = getattr(sys.modules['__main__'], '__file__')
print(filename)

#单纯得到自己的文件名字
filename = os.path.basename(filename.replace('.py', ''))
print(filename)
setting = {
           'logpath': filename , #log
           'filename': rq + '.log'
           }

class Log(object):
    ''' '''
    def __init__(self, name):
        self.path=os.path.abspath('.')+'\\LOG';
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.filename = setting['filename']   #time.log
        self.name = name  # test
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)
        # self.fh = logging.FileHandler(self.path + self.filename)
        self.fh = logging.handlers.TimedRotatingFileHandler(self.path + '/'+self.filename, 'midnight', 1, backupCount=5, encoding=None,
                                                           delay=False, utc=False)
        self.fh.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(name)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.console = logging.StreamHandler()
        self.console.setLevel(logging.INFO)
        self.console.setFormatter(self.formatter)
        self.logger.addHandler(self.console)


    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def close(self):
        self.logger.removeHandler(self.fh)
