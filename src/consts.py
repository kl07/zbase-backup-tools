#!/usr/bin/env python26
#Description: Constants definitions

SYSLOG_TAG = 'MembaseBackup'
MBRESTORE_PIDFILE = '/var/run/mbrestore.pid'
configfile = '/etc/membase-backup/default.ini'
LAST_CHECKPOINT_FILE = '/opt/membase/membase-backup/last_closed_checkpoint'
LAST_BACKUP_CHECKPOINT_FILE = '/opt/membase/membase-backup/last_backup_checkpoint'
BACKUP_TAPNAME = 'backup'
REPLICATION_TAPNAME = 'replication'
PATH_MBBACKUP_EXEC = '/opt/membase/lib/python/mbbackup-incremental'
PATH_S3CMD_EXEC = '/usr/bin/s3cmd_zynga'
INCR_DIRNAME = 'incremental'
MASTER_DIRNAME = 'master'
PERIODIC_DIRNAME = 'daily'
MAX_BACKUP_SEARCH_TRIES = 4
PATH_MBRESTORE_EXEC = '/opt/membase/lib/python/mbadm-online-restore'
PATH_MBMERGE_EXEC = '/opt/membase/lib/python/mbbackup-merge-incremental'
MERGE_CMD = "/opt/membase/membase-backup/mbmerge-incr"
BACKUP_ROOT = "/dev/shm"
MBA_BOOTSTRAP_PATH="membase-backup/mba-hostinfo"
SPLIT_UPLOAD_CMD = "/home/mtaneja/zMBBackup-incremental/misc/split_backup.py"
DEL_COMMAND = "ls"
