#!/usr/bin/env python26
#Description: Configuration parser

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

import ConfigParser
import socket
import consts
from util import gethostname

class Config:
    def __init__(self, conf_file = None):
        self.conf_file = conf_file
        self.config = ConfigParser.ConfigParser()
        self.log_level = consts.DEFAULT_LOGLEVEL
        self.syslog_tag = consts.SYSLOG_TAG
        self.membase_db_paths = consts.DB_PATHS

    def read(self):
        self.config.read(self.conf_file)
        self.backup_interval_mins = int(self.config.get('backup', 'interval'))
        try:
            self.localsnapshot_path = self.config.get('backup', 'localsnapshot_path')
        except:
            self.localsnapshot_path = consts.LOCAL_BACKUP_PATH

        try:
            self.localsnapshot_count = int(self.config.get('backup', 'localbackup_count'))
        except:
            self.localsnapshot_count = consts.LOCAL_BACKUP_COUNT


        try:
            self.master_backup_interval_days = int(self.config.get('backup', 'full_backup_interval'))
        except:
            self.master_backup_interval_days = 0

        self.upload_interval_mins = int(self.config.get('backup', 'upload_interval'))
        log_level = self.config.get('log', 'level')
        if log_level in ['INFO', 'DEBUG']:
            self.log_level = log_level

        self.syslog_tag = self.config.get('log', 'syslog_tag')
        if self.config.has_option('general', 'disk_mapper_server'):

            self.disk_mapper_server = self.config.get('general', 'disk_mapper_server')
        else:
            self.disk_mapper_server = 'UNKNOWN'

        self.buffer_list = self.config.get('general', 'buffer_list')
        if self.config.has_option("general", "membase_db_paths"):
            self.membase_db_paths = self.config.get('general', 'membase_db_paths').split(',')

        try:
            hostname = self.config.get('restore', 'hostname')
        except:
            hostname = gethostname()
        self.hostname = hostname

        self.download_retries = int(self.config.get('restore', 'download_retries'))
        self.upload_retries = int(self.config.get('backup', 'upload_retries'))

        self.disk_mapper = self.config.get('backup', 'disk_mapper')
        self.vbs = self.config.get('backup', 'vbs')

        if 'blobrestore' in self.config.sections():
            self.parallel_jobs = int(self.config.get('blobrestore',
                    'parallel_jobs'))

        try:
            self.zruntime_user = self.config.get('zruntime', 'user')
            self.zruntime_passwd = self.config.get('zruntime', 'passwd')
            self.zruntime_namespace = self.config.get('zruntime', 'namespace')
        except:
            self.zruntime_user = None
            self.zruntime_passwd = None
            self.zruntime_namespace = None

        try:
            self.zruntime_mapperkey = self.config.get('zruntime', 'mapper_key')
        except:
            self.zruntime_mapperkey = consts.ZRT_MAPPER_KEY

        try:
            self.parallel_daily_jobs = int(self.config.get('daily_merge', 'parallel_processes'))
        except:
            self.parallel_daily_jobs = consts.MAX_DAILYJOBS

        try:
            self.daily_mem_threshold = int(self.config.get('daily_merge', 'free_memory_threshold'))
        except:
            self.daily_mem_threshold = consts.DAILYJOB_MEM_THRESHOLD

        try:
            self.parallel_master_jobs = int(self.config.get('master_merge', 'parallel_processes'))
        except:
            self.parallel_master_jobs = consts.MAX_MASTERJOBS

        try:
            self.master_mem_threshold = int(self.config.get('master_merge', 'free_memory_threshold'))
        except:
            self.master_mem_threshold = consts.MASTERJOB_MEM_THRESHOLD


