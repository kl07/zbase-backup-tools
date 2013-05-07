Membase 1.7.3 Backup Tools
==========================
This repo contains membase 1.7.3 backup and restore Tools



## Backup Daemon

    python26 backupd.py -v <vbs server:port> -s <storage server IP/hostname>

1. backup daemon will connect to membase servers and create backups
2. Every 60 minutes the backup daemon will schedule incremental backups

## Restore Daemon

    python26 file_server.py (default port 22122)

## TODO

1. Integrate both backup daemon and Restore daemon in a single process
2. When a restore is in progress it should pause the backup creation for that disk
3. Update dirty index when when backups are created
4. If the backup creation fails rollback the checkpoint id of the backup cursor
5. Pause coalescers when backup or restore is in progress
6. Multiprocessing backup daemon, use multiple cores (done)
7. Change commands.getstatusoutput to use util.getcommandoutput
8. change vbs_agent.c to use to use pythong vbs agent from VBA
