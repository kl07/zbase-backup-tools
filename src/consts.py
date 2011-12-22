#!/usr/bin/env python26
#Description: Constants definitions

SYSLOG_TAG = 'MembaseBackup'
configfile = '/etc/membase-backup/default.ini'
BACKUP_TAPNAME = 'backup'
PATH_MBBACKUP_EXEC = '/opt/membase/lib/python/mbbackup-incremental'
PATH_S3CMD_EXEC = '/usr/bin/s3cmd_zynga'
INCR_DIRNAME = 'incremental'
MASTER_DIRNAME = 'master'
PERIODIC_DIRNAME = 'daily'
MAX_BACKUP_SEARCH_TRIES = 4
PATH_MBRESTORE_EXEC = '/opt/membase/lib/python/mbadm-online-restore'
PATH_MBMERGE_EXEC = '/opt/membase/lib/python/mbbackup-merge-incremental'
