#!/usr/bin/env python26
#Description: Logger functions for Backup log

#   Copyright 2013 Zynga Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

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

