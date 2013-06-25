ZBase 1.9 Backup Tools
======================

This repo contains zbase 1.9 multi-vbuckets mode incremental backup, restore
and blobrestore tools.

## Backup Daemon

    python26 backupd.py -v <vbs server:port> -s <storage server IP/hostname>

1. backup daemon will connect to membase servers and create backups
2. Every 60 minutes the backup daemon will schedule incremental backups

## Restore Daemon

    python26 file_server.py (default port 22122)

