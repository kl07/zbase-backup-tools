#!/usr/bin/env python26
import sys
sys.path.insert(0,'../')
from backuplib import BackupFactory


if __name__ == '__main__':

    class L:
        def log(self,msg):
            print msg

    logger = L()
    base_filepath = "test-%.mbb"
    backup_type = "full" # full or incr
    tapname = "backup"
    txn_size = 7000
    bo = BackupFactory(base_filepath, backup_type, tapname, logger, '0', 11212, txn_size)
    while not bo.is_complete():
        print bo.create_next_split('/tmp/')
        #create file at /dev/shm/test/test-%.mbb

    print bo.list_splits()

