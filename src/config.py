#!/usr/bin/python
#Description: Configuration parser

import ConfigParser

class Config:
    def __init__(self, conf_file = None):
        self.conf_file = conf_file 
        self.config = ConfigParser.ConfigParser()
        self.db_backup_root = '/db_backup'
        self.s3bucket = 'membase-backup'
        self.upload_check_interval_mins = 5
        self.log_level = 'INFO'
        self.syslog_tag = 'Membase-Backup-Incr'

    def read(self):
        self.config.read(self.conf_file)
        db_backup_root = self.config.get('Backup', 'db_backup_root')
        s3bucket = self.config.get('S3Upload', 's3bucket')
        upload_check_interval_mins = self.config.get('S3Upload', 'upload_check_interval_mins')
        log_level = self.config.get('Log', 'log_level')
        syslog_tag = self.config.get('Log', 'syslog_tag')

        self.db_backup_root = db_backup_root
        self.s3bucket = s3bucket
        self.upload_check_interval_mins = upload_check_interval_mins
        if log_level in ['INFO', 'DEBUG']:
            self.log_level = log_level
        self.syslog_tag = syslog_tag
        
