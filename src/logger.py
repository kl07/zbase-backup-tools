#!/usr/bin/env python26
#Description: Logger functions for Backup log

import syslog
import consts
import os

class Logger:
    """
    Logger class
    """
    def __init__(self, tag = consts.SYSLOG_TAG, level = 'INFO', meta = None):
        self.pid = os.getpid()
        self.meta = meta
        syslog.openlog(tag)
        self.silent = False

    def set_silent(self):
        self.silent = True

    def log(self, msg):
        if not self.silent:
            print str(msg)

        syslogmsg = "PID=%d " %self.pid
        if self.meta:
            syslogmsg += "%s " %self.meta
        syslogmsg += str(msg)
        syslog.syslog(syslogmsg)

    def info(self, msg):
        self.log("INFO: %s" %msg)

    def error(self, msg):
        self.log("ERROR: %s" %msg)

    def warning(self, msg):
        self.log("WARNING: %s" %msg)

