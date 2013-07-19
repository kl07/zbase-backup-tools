ZBase 1.9 Backup Tools
======================

This repo contains zbase 1.9 multi-vbuckets mode incremental backup, restore
and blobrestore tools.

### Overview

ZBase incremental backup system consists of vbucket level backup server,
vbucket restore script, backup merge daemon and blobrestore utilities. Backup daemon runs on storage
servers. It contacts vbucketserver and diskmapper to obtain vbucket to zbase
server mapping as well as vbucket to storageserver-disk mapping. Every one hour
interval, backup daemon connects to zbase servers and all vbuckets are backed
up from replica vbucket. A backup merge scheduler runs on all the storage
server, which merges and deduplicate incremental backups for the day and
converts to daily backups. Every seven days, last seven daily backups are
merged to form a master backup. In an event of server down/vbucket down,
restore script is executed. The restore script contacts the diskmapper to find
out the corresponding storage server and downloads master backup, daily backups
and recent incremental backups and performs server/vbucket level restore. The
blobrestore utilities provide a way to restore individual blobs to a given date
or nearest available backed up time in an event of blobcorruption or for
debugging purpose. A blobrestore client is available with this package which
can issue a distributed job among storage servers and consolidate the result.

### How to build ?

    Build package rpm
    $ make

### How to install ?

    Backup tools requires finite amount of tmpfs for its operation. This is an
    optimization to avoid disk read/write operations and parallel upload/download.
    This package is installed on storage servers as well as zbase servers

    Create tmpfs partition
    # mkdir /db_backup
    # mount -t tmpfs none /db_backup

    Install package
    # rpm -i zbase-backup-tools.rpm

    Edit config
    # vim /etc/zbase-backup/default.ini

    Storage server setup
    # /etc/init.d/zbase_backupd start
    # /etc/init.d/backup_merged start
    # /etc/init.d/blobrestored start

    ZBase server - for restore
    # /opt/zbase/zbase-backup/zbase-restore

### Notes

Backup tools v2.0 has a bit different architecture. IBR 2.0 (Incremental Backups and Restore)
operates on server level rather than vbucket level restore. Backup daemon runs
on each of the zbase servers. Storage servers run merge daemon and blobrestore
daemon. Blobrestore client can be run from anywhere by installing zbase-backup-tools package.



