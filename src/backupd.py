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

import sys, os, time
import thread, threading
import backuplib
from logger import Logger
from initdaemon import InitBackupDaemon
#import Queue
import backuplib
from backuplib import BackupFactory, ConnectException
import commands
import util
from util import setup_sqlite_lib, getcommandoutput, get_checkpoints_frombackup
from util import natural_sortkey, gethostname, pause_coalescer, resume_coalescer
import pdb
import sqlite3
import getopt
import consts
import multiprocessing
from multiprocessing import Process, Queue
import subprocess
from file_server import FileServer

class Backupd:

    """
        main backupd class.
        Initialize the backupd, which will connect to vbs and diskmapper
        Wake up every hour and create an incremental for all the vbuckets
        present on this storage server.


    """

    def __init__(self, vbs_host, dm_host):

        self.logger = Logger("vBucketBackupd", "INFO")
        self.logger.log("Info: ===== Starting vbucket backup daemon ====== ")
        self.initbackupd = InitBackupDaemon(dm_host, vbs_host)

        self.vb_disk_map = self.initbackupd.generate_disk_map()

        if self.vb_disk_map == None:
            self.logger.log("Critical: Failed to init backup daemon. Exiting... ")
            sys.exit(1)

        #spawn a process for the restore daemon
        fs = Process(target=self.start_restore_daemon, args=(dm_host,))
        fs.start()

        #backup daemon initialized. We are all set to go
        self.main_loop()
        fs.join()


    def start_restore_daemon(self, dm_host):

        file_server = FileServer(dm_host,"0.0.0.0", 22122)
        file_server.start()


    def main_loop (self):

        """
            These set of tasks will be run in a separate thread, one thread
            per disk.
            read local manifest db
            1. get checkpoint id of each vbucket_id that we want to backup
            2. check if there is a named cursor registered.
            3. if there is no named cursor registered then check if the checkpoint
               from which we want to resume the backup exists.
            4. if not then backfill needs to be done.
            5. to force a backfill for a vbucket_id we need to delete the named
               cursor.

        """

        self.thread_queue = {}
        self.task_queue = {}
        while (1):

            self.vb_disk_map = self.initbackupd.generate_disk_map()
            self.workqueue = self.create_workqueue(self.vb_disk_map)
            if self.workqueue == {}:
                self.logger.log("Fatal: Unable to create workqueue, exiting...")
                return

            #start a backup task for each disk id in the workqueue list
            for disk_id in self.workqueue.keys():
                if disk_id in self.thread_queue:
                    continue
                self.task_queue[disk_id] = multiprocessing.Queue()
                self.thread_queue[disk_id] = backup_thread(self.task_queue[disk_id], self.logger, disk_id)
                self.thread_queue[disk_id].start()


            for disk_id in self.workqueue.keys():
                backup_thd = self.thread_queue[disk_id]
                vb_path_list = self.workqueue[disk_id]
                task_queue = self.task_queue[disk_id]
                for vb_path in vb_path_list:
                    #backup_thd.queue_task(vb_path)
                    task_queue.put_nowait(vb_path)

            time.sleep (consts.BACKUP_INTERVAL)


    """
    def schedule_backup_task(self, backup_thread, vb_backup_task)

        backup_thread.queue_task(vb_backup_task)

    """

    ## create a workqueue sorted by the disk id
    def create_workqueue (self, vb_disk_map):

        workqueue = {}

        for vb_path in vb_disk_map:
            path = vb_path['path'].split('/')
            disk_id = path[1]

            workqueue_list = []

            try:
                workqueue_list = workqueue[disk_id]

            except:
                workqueue_list = []
                workqueue[disk_id] = workqueue_list

            workqueue_list.append(vb_path)

        return workqueue


#one backup process per disk
class backup_thread(multiprocessing.Process) :

    def __init__ (self, task_queue, logger, disk_id):

        multiprocessing.Process.__init__(self)
        self.disk_id = disk_id
        self.task_queue = task_queue
        self.logger = logger
        self.tapname = ""
        self.backup_name = ""
        self.host="localhost"
        self.port = 11211

    def run(self):

        self.logger.log ("Info: Starting backup thread for disk_id %s" %self.disk_id)
        self.coalescer_paused = False
        while 1:

            if self.task_queue.empty() == True and self.coalescer_paused == True:
                #resume the coalescer
                util.resume_coalescer(self.logger, self.disk_id)
                self.coalescer_paused = False

            vb_backup_task = self.task_queue.get(True, None)
            # suspend coalescing operation
            util.pause_coalescer(self.logger, self.disk_id)
            self.coalescer_paused = True

            #schedule backup task for this process
            print ("\ngot backup task ", vb_backup_task)

            status = self.init_backup(vb_backup_task)
            self.last_checkpoint_file = vb_backup_task['path'] + "/vbid_" + str(vb_backup_task['vb_id']) + "_" + consts.LAST_CHECKPOINT_FILE
            if status == False:
                continue
            now = time.gmtime(time.time())
            datetimestamp = time.strftime('%Y-%m-%d %H:%M:%S', now)
            self.backup_name = time.strftime('backup-%Y-%m-%d_%H:%M:%S-%.mbb',now)

            status = self.attach_checkpoint_cursor(vb_backup_task)
            if status == False:
                self.logger.log("Failure: could not create temporary backup cursor. skipping backup for vb %d" %vb_backup_task[vb_id])
                continue

            status = self.take_backup(vb_backup_task, datetimestamp)

            status = self.cleanup_checkpoint_cursor(vb_backup_task, status)
            if status == False:
                self.logger.log("Failure: Failed to cleanup temporary backup cursor or restore cursor. Manual intervention required")

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass

    ## add a temporary backup cursor to the last backed up checkpoint,
    ## if the backup is successful we will delete this cursor so that the
    ## so that the backed up checkpoints can be deleted.
    def attach_checkpoint_cursor(self, vb_backup_task):

        if os.path.exists(self.last_checkpoint_file) and self.backup_type != 'full':
            f = open(self.last_checkpoint_file)
            self.last_backup_checkpoint = int(f.read())
            f.close()
        else:
            self.last_backup_checkpoint = str(1)

        self.temporary_checkpoint_cursor = "temporary_backup_cursor_vb_" + str(vb_backup_task['vb_id'])
        checkpoint_add_cmd = "python26 mbadm-tap-registration -h " + self.host + ":" + str(self.port) + " -r " + \
        self.temporary_checkpoint_cursor + " -l " + self.last_backup_checkpoint + " -v " + str(vb_backup_task['vb_id'])

        status, output = commands.getstatusoutput(checkpoint_add_cmd)
        if status > 0:
            self.logger.log("Critical: Failed to register temporary backup cursor \nCommand: %s Output %s" %(checkpoint_add_cmd, output))
            return False

        return True

    # on successful backup, delete the temporary cursor.
    # if the backup has failed, delete the current cursor and reregister to the last backup cursor
    # and then delete the temporary cursor
    def cleanup_checkpoint_cursor(self, vb_backup_task, backup_status):

        delete_tmp_cursor_cmd = "python26 mbadm-tap-registration -h " + self.host +":" + str(self.port) + " -d " + self.temporary_checkpoint_cursor

        if backup_status == True:
            status, output = commands.getstatusoutput(delete_tmp_cursor_cmd)
            if status > 0:
                self.logger.log("Failure: Unable to delete temporary backup cursor \nCommand: %s Output %s" %(delete_tmp_cursor_cmd, output))
                return False
            return True
        else:
            #failed backup
            delete_tap_cursor_cmd = "python26 mbadm-tap-registration -h " + self.host + ":" + str(self.port) + " -d " + self.tapname
            status, output = commands.getstatusoutput(delete_tap_cursor_cmd)
            if status > 0:
                self.logger.log("Failure: Unable to delete backup cursor. \n Command: %s Output %s" %(delete_tap_cursor_cmd, output))
                return False

            #reattach the tap cursor to the original checkpoint id
            checkpoint_add_cmd = "python26 mbadm-tap-registration -h " + self.host + ":" \
            + str(self.port) + " -r " + self.tapname + " -l " + str(self.last_backup_checkpoint) \
            + " -v " + str(vb_backup_task['vb_id'])

            status, output = commands.getstatusoutput(checkpoint_add_cmd)
            if status > 0:
                self.logger.log("Failure: Unable to restore backup cursor. \n Command: %s Output %s" %(checkpoint_add_cmd, output))
                return False

            #yank the temporary backup cursor
            status, output = commands.getstatusoutput(delete_tmp_cursor_cmd)
            if status > 0:
                self.logger.log("Failure: Unable to delete temporary backup cursor \nCommand: %s Output %s" %(delete_tmp_cursor_cmd, output))
                return False
            return True


    def take_backup(self, vb_backup_task, datetimestamp):

        """
        Create incremental backup
        """
        total_size = 0
        size = 0
        checkpoints = []

        start_time = time.time()
        self.logger.log("==== START BACKUP ====")
        self.logger.log("Creating %s Backup for %s and vbucket %d " %(self.backup_type, self.backup_name, vb_backup_task['vb_id']))
        self.vb_id = []
        self.vb_id.append(vb_backup_task['vb_id'])
        dirty_file_list = []

        dirty_little_tokens = vb_backup_task['path'].split('/')
        disk_id = dirty_little_tokens[1]
        dirty_file_path = "/" + disk_id + "/" + consts.DIRTY_DISK_FILE

        if self.backup_type == "full":
            now = time.gmtime(time.time())
            date_today = time.strftime('%Y-%m-%d', now)
            backup_path = vb_backup_task['path'] + "/master" + "/" + date_today + "/"

            if os.path.exists(backup_path) == False:
                try:
                    # equivalent of mkdir -p SEG-11168
                    self.mkdir_p(backup_path)
                except Exception, e:
                    self.logger.log("Failed: Unable to create directory %s " %backup_path)
                    return False

            #check if done file exists, if so then skip master backup
            done_file = backup_path + "done"
            if os.path.exists(done_file) == True:
                self.logger.log("Failed: Done file exists, please clear master backup directory %s" %backup_path)
                return False


        else:
            backup_path =  vb_backup_task['path'] + "/incremental/"

        for r in range(consts.BACKUP_RETRIES):
            retry = False
            try:
                bf_instance = BackupFactory(self.backup_name, self.backup_type,
                        self.tapname, self.vb_id, self.logger, self.host, int(self.port))
            except Exception, e:
                self.logger.log("Failure: Initializing backup factory instance backup path %s" %vb_backup_task['path'])
                self.logger.log(str(e))
                return False

            while not bf_instance.is_complete():
                try:
                    filepath = bf_instance.create_next_split(backup_path)
                    size = os.stat(filepath).st_size
                    total_size += size

                    if size <= 4096:
                        if not self._is_backup_valid(filepath):
                            self.logger.log("Failure: Backup size is %d, backup path %s" %(size, vb_backup_task['path']))
                            raise Exception("Backup is invalid")

                    try:
                        checkpoints.extend(get_checkpoints_frombackup(filepath))
                    except Exception, e:
                        self.logger.log("Failure: sqlite file %s is corrupt (%s), backup path %s" %(self.backup_name, str(e), vb_backup_task['path']))
                        raise Exception("Sqlite file corrupt")

                    retry = False
                except ConnectException:
                    retry = True
                    time.sleep(1)
                    break

                except Exception, e:
                    self.logger.log(str(e))
                    self.logger.log("Failure: Creating Backup for %s, backup path %s" %(self.backup_name, vb_backup_task['path']))
                    split_file = bf_instance.get_current_split()
                    if split_file:
                        self._remove_file(split_file)

                    return False

            if retry:
                continue
            else:
                break

        if retry:
            self.logger.log("FAILED: TAP connection retrying failed (5), backup path %s" %vb_backup_task['path'])
            return False

        checkpoints.sort()
        dirty_file_list.append(self.last_checkpoint_file)
        if os.path.exists(self.last_checkpoint_file) and self.backup_type != 'full':
            f = open(self.last_checkpoint_file)
            last_backup_checkpoint = int(f.read())
            f.close()
        else:
            last_backup_checkpoint = 0

        if len(checkpoints):
            if last_backup_checkpoint > 0:
                if last_backup_checkpoint + 1 != checkpoints[0]:
                    self.logger.log("FAILED: Invalid backup. Last backup checkpoint = %d, New backup checkpoint = %s backup path %s" %(last_backup_checkpoint, checkpoints[0], vb_backup_task['path']))
                    return False
                else:
                    self.logger.log("Last backup_checkpoint: %d Current backup checkpoints: %s" %(last_backup_checkpoint, str(checkpoints)))

            f = open(self.last_checkpoint_file, 'w')
            f.write(str(checkpoints[-1]))
            f.close()
            split_files = map(lambda x: os.path.basename(x), map(lambda y: y[-1], bf_instance.list_splits()))
            split_index_path = "%s.split" %"-".join(filepath.split('-')[:-1])
            fd = open(split_index_path, 'w')
            for split_file in split_files:
                fd.write("%s\n" %split_file)
                dirty_file_list.append(os.path.join(backup_path, split_file))
            fd.close()

            #finally append the split file to the dirty file list

            #if the type of backup is full, then add a done file
            if self.backup_type == "full":
                done_file = backup_path + "done"
                fd = open(done_file, 'w')
                fd.write("0")
                fd.close()
                dirty_file_list.append(done_file)

            #update the list of files that have been created
            self.logger.log("Info: updating dirty file %s" %dirty_file_path)
            status = util.appendToFile_Locked(dirty_file_path, dirty_file_list)
            if status == False:
                self.logger.log("Warning: failed to update dirty file %s" %dirty_file_path)

        else:
            self.logger.log("FAILED: Current backup contains zero checkpoints backup path %s" %vb_backup_task['path'])
            return False

        end_time = time.time()
        time_taken = end_time - start_time
        self.logger.log("Completed Backup for %s" %(datetimestamp))
        self.logger.log("BACKUP SUMMARY: type:%s size:%d, time-taken: %d, backup-file:%s split-count:%d" %(self.backup_type, total_size, int(time_taken), self.backup_name, len(bf_instance.list_splits())))
        self.logger.log("==== END BACKUP ====")
        return True

    def _is_backup_valid(self, filepath):
        try:
            db = sqlite3.connect(filepath)
            cursor = db.execute("select count(*) from cpoint_op;")
            mutation_count = cursor.fetchone()[0]
            if mutation_count == 0:
                return False
            else:
                return True

        except Exception, e:
            self.logger.log("FAILED: sqlite validation failed for %s (%s)" %(filepath, str(e)))
            return False


    def _remove_file(self, filepath):
        self.logger.log("Removing file, %s " %filepath)
        try:
            os.unlink(filepath)
            return True
        except Exception, e:
            self.logger.log("FAILED: Unable to remove file %s (%s)" %(filepath, str(e)))
            return False

    def init_backup(self, vb_backup_task):

        try:
            self.host,self.port = vb_backup_task['server'].split(':')
        except:
            self.logger.log("Critical: Invalid server name %s" %vb_backup_task['server'])
            return False

        self.tapname = "backup_" + str(vb_backup_task['vb_id']) + "_cursor"

        tapname_check_cmd = "echo stats checkpoint | nc " + self.host + " " + self.port + " | grep " + self.tapname
        status,output = commands.getstatusoutput(tapname_check_cmd)

        if status != 0 or output == '':
            self.logger.log("Info: Tap name not found %s" %self.tapname)
            membase_check_cmd = "echo version | nc %s %s" %(self.host,self.port)
            status,ouput = commands.getstatusoutput(membase_check_cmd)
            if status != 0:
                self.logger.log("Failure: Server not running on %s:%s" %(self.host, self.port))
                return False
            else:
                ## register a backup cursor
                self.backup_type = "full"
        else:
            self.backup_type = "incremental"

        return True

    def exit(self, status):
        sys.exit(status)

    def queue_task(self, vb_backup_task):

        if vb_backup_task != None:
            self.task_queue.put_nowait(vb_backup_task)

        return



if __name__ == '__main__':


    options, remainder = getopt.getopt(sys.argv[1:], 'v:d:', ['vbs_host=',
                                                              'disk_mapper=',
                                                             ])
    vbs_host = ""
    disk_mapper = ""
    for opt, arg in options:
        if opt in ('-v', '--vbs'):
            vbs_host = arg
        elif opt in ('-d', '--disk_mapper'):
            disk_mapper = arg


    if vbs_host == "" or disk_mapper == "":
        print "Need to specify vbs server and disk_mapper server"
        print "Usage: backupd.py -v <vbs server:port> -d <disk mapper>"
        sys.exit(1)

    backupd_process = Backupd(vbs_host, disk_mapper)

