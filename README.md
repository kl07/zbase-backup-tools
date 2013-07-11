ZBase 1.9 Backup Tools
======================

This repo contains zbase 1.9 multi-vbuckets mode incremental backup, restore
and blobrestore tools.

## Backup & restore Daemon

    sudo python26 backupd.py start
    sudo python26 backupd.py stop

1. backup daemon will connect to zbase servers and create backups
2. Every 60 minutes the backup daemon will schedule incremental backups
3. restore process will listen on port 22122


