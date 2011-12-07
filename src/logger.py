#!/usr/bin/python 
#Description: Logger functions for Backup log 

import syslog 
import consts

class Logger:
    """
    Logger class
    """
    def __init__(self, tag = consts.SYSLOG_TAG, level = 'INFO'):
	syslog.openlog(tag)

    def log(self, msg):
        print str(msg)
	syslog.syslog(str(msg))

