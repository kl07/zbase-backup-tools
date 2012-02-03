#!/usr/bin/python26
#Description: Merge library to merge daily and master backups into a single backup

import time
import os
import consts
import signal
import datetime
import Queue
import sys
import getopt
from threading import Thread
from logger import Logger
from config import Config
from util import natural_sortkey, setup_sqlite_lib, getcommandoutput

#Setup ld_path for sqlite3
setup_sqlite_lib()

def parse_args():
    options = {'exclude_list':None}
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:o:l:x:d:t:')
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    for o,a in opts:
        if o == '-h':
            options['hostname'] = a
        elif o == '-o':
            options['output'] = a
        elif o == '-l':
            options['filelog'] = a
        elif o == '-x':
            options['exclude_list'] = a
        elif o == '-t':
            options['btype'] = a
        elif o == '-d':
            options['date'] = a
        else:
            assert False, "Unknown option"

    if len(options) < 5:
        usage("More arguments required")

    elif options['btype'] == consts.MASTER_DIRNAME:
        if not options.has_key('date'):
            usage("Date parameter is missing")

    return options

def usage(msg):
    print "Usage: %s -h hostname -t daily [ -d date ] -l s3list.txt [ -x exclude_list.txt ] -o merged_file.mbb" %sys.argv[0]
    print msg
    sys.exit(2)

class Merge:

    def __init__(self, options):
        self.logger = options['logger']
        self.output_file = options['output']
        if options.has_key('filelog'):
            self.filelog = options['filelog']
        else:
            self.filelog = None

        if options.has_key('exclude_list'):
            self.exclude_list = options['exclude_list']
        else:
            self.exclude_list = None

        self.btype = options['btype']
        self.hostname = options['hostname']
        if self.btype == consts.MASTER_DIRNAME:
            self.date = options['date']
        self.cloud = options['cloud']
        self.game_id = options['game_id']
        self.download_retries = options['download_retries']
        self.download_threads_count = options['download_threads_count']

        if options.has_key('buffer_list'):
            buffer_list = options['buffer_list']
        else:
            self.buffer_count = options['buffer_count']
            self.buffer_path = options['buffer_path']
            for i in range(self.download_threads_count):
                buffer_list = os.path.join(self.buffer_path, str(i))

        self.processlist = Queue.Queue()
        self.exit_status = 0
        self.merge_complete = False

        self.thread_count = len(buffer_list)
        self.free_buffer_list = Queue.Queue()
        self.download_queue = Queue.Queue()
        self.merge_queue = Queue.Queue()
        self.file_count = 0
        for b in buffer_list:
            self.free_buffer_list.put(b)

        self.threads = []

    def kill_subprocesses(self, sig=None, frame=None):
        for process in self.processlist.queue:
            try:
                process.terminate()
            except:
                pass

        for t in self.threads:
            try:
                t._Thread__stop()
            except:
                pass

        self.exit_status = 1

    def _list_s3_files(self, s3path, complete_check=False):
        ls_cmd = "%s ls %s" %(consts.PATH_S3CMD_EXEC, s3path)
        self.logger.log("Executing command %s" %ls_cmd)
        status, output = self.getstatusoutput(ls_cmd)
	if status !=0:
            self.logger.log("FAILED: %s" %output)
            return False
	else:
            lines = output.split('\n')
            files = map(lambda x: 's3://'+x.split('s3://')[-1], lines)
            complete_backup = False

            for f in files:
                if f.endswith('.done'):
                    complete_backup = True

            mbb_files = filter(lambda x: x.endswith('.mbb'), files)
            if len(mbb_files) and complete_check and complete_backup == False:
                self.logger.log("Files found, but .done marker cannot be found in the backup directory")
                return False

            return mbb_files

    def getstatusoutput(self, cmd):
        return getcommandoutput(cmd, self.processlist)

    def _download_file(self, s3path, filepath):
        get_cmd = "%s sync %s %s" %(consts.PATH_S3CMD_EXEC, s3path, filepath)
        retries = self.download_retries
        self.logger.log("Executing command %s" %get_cmd)
        for i in range(retries):
            if i > 0:
                self.logger.log("Retrying download for %s" %s3path)
            status, output = self.getstatusoutput(get_cmd)
            self.logger.log("Downloading file %s to %s" %(s3path, filepath))
            if status == 0:
                break

    	if status !=0:
            self.logger.log("FAILED: %s" %output)
            return False
        else:
            self.logger.log("Completed downloading file %s" %s3path)
            return True

    def fetch_backuplist(self):
        """
        Get the list of backup files which are to be downloaded
        """

        backup_list = []
        self.logger.log("Fetching Backup list from S3")
        if self.btype == consts.PERIODIC_DIRNAME:
            backup_list = self._list_s3_files('s3://%s/%s/%s/%s/' %(self.game_id, self.hostname, self.cloud, consts.INCR_DIRNAME))
            if self.exclude_list:
                f = open(self.exclude_list)
                exclude_list_items = map(lambda x: x.strip(), f.readlines())
            else:
                exclude_list_items = []

            incremental_backup_list = list(set(backup_list) - set(exclude_list_items))
            self.logger.log("Fetching list of incremental backups")
            if backup_list == False:
                return False

            if backup_list == []:
                self.logger.log("Could not find any backups")
                return []

            self.backup_list = incremental_backup_list
	    return [ x for x in reversed(sorted(incremental_backup_list, key=natural_sortkey)) ]

        elif self.btype == consts.MASTER_DIRNAME:
            year, month, day = map(lambda x: int(x), self.date.split('-'))
            datetime_object = datetime.date(year, month, day)
            for i in range(consts.MAX_BACKUP_LOOKUP_DAYS+1):
                difference = datetime.timedelta(days=-(i))
                datestamp = (datetime_object + difference).strftime('%Y-%m-%d')

                master_list = self._list_s3_files('s3://%s/%s/%s/%s/%s/'
                        %(self.game_id, self.hostname, self.cloud,
                            consts.MASTER_DIRNAME, datestamp), True)
                if master_list == False:
                    return False

                if len(master_list) and i==0:
                    break

                if len(master_list) == 0:
                    periodic_list = self._list_s3_files('s3://%s/%s/%s/%s/%s/'
                            %(self.game_id, self.hostname, self.cloud,
                                consts.PERIODIC_DIRNAME, datestamp), True)

                    if periodic_list == False:
                        return False

                    backup_list.extend(periodic_list)

                else:
                    backup_list.extend(master_list)
                    break

            self.backup_list =  backup_list
            if not backup_list:
                self.logger.log("Could not find any backups")

            return backup_list


    def populate_queue(self):
        backup_files = self.fetch_backuplist()
	if backup_files == False:
		return False

        for f in backup_files:
            self.logger.log("Found file %s" %f)

        for i,f in enumerate(backup_files):
            self.download_queue.put((i, f, 'backup-%05d.mbb' %i))

        self.file_count = len(self.download_queue.queue)

    def _do_merge(self, backup_file):
        self.logger.log("Performing merge using from %s" %backup_file)
        merge_cmd = "python26 %s -f -o %s %s" %(consts.PATH_MBMERGE_EXEC, self.output_file, backup_file)
        self.logger.log("Executing command %s" %merge_cmd)
        status, output = self.getstatusoutput(merge_cmd)
        if status == 0:
            return True
        else:
            self.logger.log("FAILED: Executing command %s (%s)"  %(merge_cmd, output))
            return False

    def perform_merge(self):
        shard = 0
        while shard < self.file_count:
            self.logger.log("Checking for backup file with shard %d" %shard)
            merge_list = list(self.merge_queue.queue)
            merge_list.sort()
            if len(merge_list) > 0 and shard == merge_list[0][0]:
                backup = merge_list[0]
                self.merge_queue.queue.remove(backup)
                backup_file = backup[1]
                buffer_path = backup[2]
                status = self._do_merge(backup_file)
                if status:
                    os.unlink(backup_file)
                    self.free_buffer_list.put(buffer_path)
                    shard +=1
                else:
                    self.exit_status = 1
                    return

            time.sleep(2)
        self.merge_complete = True

    def download_files(self):
        while True:
            self.logger.log("Attempt to obtain a file for download")
            backup = self.download_queue.get()
            self.logger.log("SUCCESS: Attempt to obtain a file for download %s" %str(backup))
            buffer_path = self.free_buffer_list.get()
            self.logger.log("Obtained buffer %s" %buffer_path)
            status = self._download_file(backup[1], '%s/%s' %(buffer_path, backup[2]))
            self.logger.log("Download complete")
            if status:
                self.merge_queue.put((backup[0], '%s/%s' %(buffer_path, backup[2]), buffer_path))
            else:
                self.exit_status = 1
                return

            self.download_queue.task_done()

    def execute(self):
        status = []
        if self.populate_queue() == False:
            return [False]

        for i in range(self.thread_count):
            t = Thread(target=self.download_files)
            t.daemon = True
            t.start()
            self.threads.append(t)

        t = Thread(target=self.perform_merge)
        t.daemon = True
        t.start()
        self.threads.append(t)

        while not self.merge_complete:
            if self.exit_status !=0:
                self.kill_subprocesses()
                return [False]

            time.sleep(1)

        if self.filelog:
            f = open(self.filelog, 'w')
            for i in self.backup_list:
                f.write(i+'\n')
            f.close()

        return [True, self.backup_list]

if __name__ == '__main__':
    try:
        config = Config(consts.CONFIG_FILE)
        config.read()
        logger = Logger(tag = config.syslog_tag, level = config.log_level)
    except Exception, e:
        config.syslog_tag = consts.SYSLOG_TAG
        logger = Logger(tag = syslog_tag, level = config.log_level)
        logger.log("FAILED: Parsing config file (%s)" %(str(e)))

    if os.path.exists(consts.MBMERGE_PID_FILE):
        pid = int(open(consts.MBMERGE_PID_FILE, 'r').read())
        try:
            os.kill(pid, 0)
            logger.log("Merge process is already running with PID %d" %pid)
            sys.exit(1)
        except:
            pass

    options = parse_args()
    options['logger'] = logger
    options['cloud'] = config.cloud
    options['game_id'] = config.game_id
    options['download_retries'] = config.download_retries
    options['buffer_list'] = config.buffer_list.split(',')
    options['download_threads_count'] = len(options['buffer_list'])

    fd = open(consts.MBMERGE_PID_FILE,'w')
    fd.write(str(os.getpid()))
    fd.close()

    merge = Merge(options)
    for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
        signal.signal(sig, merge.kill_subprocesses)


    status = merge.execute()
    if not status[0]:
        logger.log("Merge failed")         
        sys.exit(1)

    logger.log("Merge completed successfully. Merged file can be found at %s" %options['output'])

