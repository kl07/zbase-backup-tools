#!/usr/bin/env python26
import sys
sys.path.insert(0,'../')
from backuplib import BackupFactory


if __name__ == '__main__':

    class L:
        def log(self,msg):
            print msg

    logger = L()
    base_filepath = "test/test-%.mbb"
    backup_type = "full" # full or incr
    tapname = "backup"
    bo = BackupFactory(base_filepath, backup_type, tapname,logger, '0', 11211)
    while not bo.is_complete():
        print bo.create_next_split('/dev/shm')
        #create file at /dev/shm/test/test-%.mbb

