#!/usr/bin/env python26
#Description: Constants definitions

SYSLOG_TAG = 'membase-backup'
MBRESTORE_PID_FILE = '/var/run/mbrestore.pid'
MBBACKUP_PID_FILE = '/var/run/mbbackup.pid'
MBMERGE_PID_FILE = '/var/run/mbmerge.pid'
CONFIG_FILE = '/etc/membase-backup/default.ini'
LAST_CHECKPOINT_FILE = '/db/last_closed_checkpoint'
BACKUP_TAPNAME = 'backup'
REPLICATION_TAPNAME = 'replication'
PATH_MBBACKUP_EXEC = '/opt/membase/lib/python/mbbackup-incremental'
PATH_S3CMD_EXEC = '/usr/bin/s3cmd_zynga'
PATH_S3CMD_CONFIG = '/etc/membase-backup/s3cmd.cfg'
INCR_DIRNAME = 'incremental'
MASTER_DIRNAME = 'master'
PERIODIC_DIRNAME = 'daily'
MAX_BACKUP_SEARCH_TRIES = 4
PATH_MBRESTORE_EXEC = '/opt/membase/lib/python/mbadm-online-restore'
PATH_MBMERGE_EXEC = '/opt/membase/lib/python/mbbackup-merge-incremental'
S3_BUCKET = 'membase-backup'
DEFAULT_LOGLEVEL = 'INFO'
MERGE_CMD = "/opt/membase/membase-backup/mbmerge-incr"
BACKUP_ROOT = "/dev/shm"
MBA_BOOTSTRAP_PATH="membase-backup/mba-hostinfo"
SPLIT_UPLOAD_CMD = "/opt/membase/membase-backup/misc/split_backup.py"
SPLIT_SIZE = 512
DEL_COMMAND = "del"
