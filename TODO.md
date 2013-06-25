## TODO

1. Integrate both backup daemon and Restore daemon in a single process
2. When a restore is in progress it should pause the backup creation for that disk
3. Update dirty index when when backups are created (done)
4. If the backup creation fails rollback the checkpoint id of the backup cursor
5. Pause coalescers when backup or restore is in progress
6. Multiprocessing backup daemon, use multiple cores (done)
7. Change commands.getstatusoutput to use util.getcommandoutput
8. change vbs_agent.c to use to use python vbs agent from VBA
