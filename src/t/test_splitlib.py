#!/usr/bin/env python26

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

