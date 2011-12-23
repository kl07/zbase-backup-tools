#!/usr/bin/env python26
#Description: Configuration parser

import ConfigParser

class Config:
    def __init__(self, conf_file = None):
        self.conf_file = conf_file 
        self.config = ConfigParser.ConfigParser()
        self.s3bucket = 'membase-backup'
        self.log_level = 'INFO'
        self.syslog_tag = 'Membase-Backup-Incr'

    def read(self):
        self.config.read(self.conf_file)
        s3bucket = self.config.get('S3Upload', 's3bucket')
        backup_interval_mins = self.config.get('Backup', 'backup_interval_mins')
        upload_interval_mins = self.config.get('S3Upload', 'upload_interval_mins')
        log_level = self.config.get('Log', 'log_level')
        syslog_tag = self.config.get('Log', 'syslog_tag')
        cloud = self.config.get('General', 'cloud')
        game_id = self.config.get('General', 'game_id')
        buffer_list = self.config.get('General', 'buffer_list')
        hostname = self.config.get('Restore', 'hostname')
        download_retries = self.config.get('Restore', 'download_retries')
        upload_retries = self.config.get('S3Upload', 'upload_retries')

        self.s3bucket = s3bucket
        self.backup_interval_mins = int(backup_interval_mins)
        self.upload_interval_mins = int(upload_interval_mins)
        if log_level in ['INFO', 'DEBUG']:
            self.log_level = log_level
        self.syslog_tag = syslog_tag
        self.cloud = cloud
        self.game_id = game_id
        self.hostname = hostname 
        self.buffer_list = buffer_list 
        self.download_retries = int(download_retries)
        self.upload_retries = int(upload_retries)
        
