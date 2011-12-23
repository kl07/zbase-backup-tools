#!/usr/bin/env python26
#Description: Logger functions for Backup log 

import syslog 
import consts
import os

class Logger:
    """
    Logger class
    """
    def __init__(self, tag = consts.SYSLOG_TAG, level = 'INFO'):
        self.pid = os.getpid()
	syslog.openlog(tag)

    def log(self, msg):
        print str(msg)
	syslog.syslog("PID=%d %s" %(self.pid, str(msg)))

