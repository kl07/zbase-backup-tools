#!/usr/bin/env python26
#Description: Configuration parser

import ConfigParser
import socket
import consts

class Config:
    def __init__(self, conf_file = None):
        self.conf_file = conf_file 
        self.config = ConfigParser.ConfigParser()
        self.s3bucket = consts.S3_BUCKET
        self.log_level = consts.DEFAULT_LOGLEVEL
        self.syslog_tag = consts.SYSLOG_TAG

    def read(self):
        self.config.read(self.conf_file)
        self.s3bucket = self.config.get('s3', 'bucket')
        self.backup_interval_mins = int(self.config.get('backup', 'interval'))
        self.upload_interval_mins = int(self.config.get('backup', 'upload_interval'))
        log_level = self.config.get('log', 'level')
        if log_level in ['INFO', 'DEBUG']:
            self.log_level = log_level

        self.syslog_tag = self.config.get('log', 'syslog_tag')
        self.cloud = self.config.get('general', 'cloud')
        self.game_id = self.config.get('general', 'game_id')
        self.buffer_list = self.config.get('general', 'buffer_list')
        try:
            hostname = self.config.get('restore', 'hostname')
        except:
            hostname = socket.gethostname()
        self.hostname = hostname

        self.download_retries = int(self.config.get('restore', 'download_retries'))
        self.upload_retries = int(self.config.get('backup', 'upload_retries'))

        if 'blobrestore' in self.config.sections():
            self.parallel_jobs = int(self.config.get('blobrestore',
                    'parallel_jobs'))
            self.download_buffers_per_job = int(self.config.get('blobrestore',
            'download_buffers_per_job'))

