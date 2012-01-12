#!/usr/bin/env python26
#Description: Blobrestore Daemon

import Queue
import os
import time
import re
import datetime
import pickle
import sqlite3
import signal
import logging
import random
import sys
from threading import Thread, Semaphore

PYTHON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
sys.path.insert(0, PYTHON_PATH)

from config import Config
from logger import Logger
from blobrestore import NodeJob, KeyStore, download_file
from util import getcommandoutput

import consts

def init_logger():
    global logger
    logfile = consts.BLOBRESTORE_DAEMON_LOG
    logger = logging.getLogger('Blobrestore')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler_file = logging.FileHandler(logfile)
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

init_logger()

def log(msg):
    global logger
    print msg 
    logger.info(msg)

class BackupServer:
    def __init__(self, bucket, cloud_game_id, hostname):
        self.bucket = bucket
        self.cloud_game_id = cloud_game_id
        self.hostname = hostname

    def list_backups(self, start_date, days=0, master_backup=False):
        backups = []
        year, month, day = map(lambda x: int(x), start_date.split('-'))
        datetime_object = datetime.date(year, month, day)
        for i in range(days+1):
            difference = datetime.timedelta(days=-(i))
            datestamp = (datetime_object + difference).strftime('%Y-%m-%d')
            if master_backup:
                backup_types = [consts.PERIODIC_DIRNAME, consts.MASTER_DIRNAME] 
            else:
                backup_types = [consts.PERIODIC_DIRNAME] 

            for btype in backup_types:
                s3path = 's3://%s/%s/%s/%s/%s/ -r' %(self.bucket,
                        self.cloud_game_id.replace('-','/'), self.hostname, btype, datestamp)
                ls_cmd = "%s -c %s ls %s" %(consts.PATH_S3CMD_EXEC,
                        consts.PATH_S3CMD_CONFIG, s3path) 
                status, output = getcommandoutput(ls_cmd, None)
                if status != 0:
                    return False
                else:
                    lines = output.split('\n')
                    files = filter(lambda y: y.endswith('.mbb'), map(lambda x:
                        's3://'+x.split('s3://')[-1], lines))
                    for f in files:
                        regex = re.compile('-(\d+).mbb')
                        split_no = int(regex.findall(f)[0])
                        backups.append(Backup(f, datestamp, split_no))

        return backups

def create_nodejob_tracker(nodejob):
    nodejob_tracker_instance = NodeJobTracker(nodejob)
    job_count = len(nodejob.jobs)
    for i in range(job_count):
        j = Job()
        j.parse_nodejob(i, nodejob, nodejob_tracker_instance)
        nodejob_tracker_instance.jobs.append(j)

    return nodejob_tracker_instance

class NodeJobTracker:
    """
    Tracker class that keep track of restore jobs
    """
    instances = []

    def __init__(self, nj):
        self.nodejob_ref = nj
        self.job_id = nj.job_id 
        self.activated = False
        self.complete = False
        self.jobs = []
        self.__class__.instances.append(self)

    def __del__(self):
        for j in self.jobs:
            del j

    def activate(self):
        if not self.activated:
            logfile = os.path.join(self.jobs[0].dirpath, 'restore.log')
            self.dirpath = self.jobs[0].dirpath
            self.logger = logging.getLogger('Nodejob:%d' %self.job_id)
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            handler_file = logging.FileHandler(logfile)
            handler_file.setFormatter(formatter)
            self.logger.addHandler(handler_file)
            self.activated = True
            pending_file = os.path.join(consts.BLOBRESTORE_JOBS_DIR, 'pending',
            'nodejob_%s_ID%d.njob' %(self.nodejob_ref.game_id,
                self.nodejob_ref.job_id))
            try:
                os.unlink(pending_file)
            except:
                pass
            self.nodejob_ref = None

    def log(self, msg):
        self.logger.info(msg)

    def get_status(self):
        total_keys = 0
        restored_keys = 0
        remaining_keys = 0
        exit_status = 0
        complete = True
        for j in self.jobs:
            if not j.complete:
                complete = False
            status = j.get_status()
            total_keys += status[0]
            restored_keys += status[1]
            remaining_keys += status[2]
            exit_status += status[3]

        if complete:
            self.complete = True

        return total_keys, restored_keys, remaining_keys, exit_status

class Backup:
    """
    Backup class which represent sqlite backup file
    """

    def __init__(self, path, date, split_no):
        self.path = path
        self.localpath = None
        self.date = date
        self.split_no = split_no
        self.downloaded = False

    def setLogger(self, func):
        self.log = func

    def download_to_dir(self, dirpath):
        filename = self.path.split('/')[-1]
        self.localpath = os.path.join(dirpath, "%d_%s" %(random.randint(1,1000), filename))
        status = download_file(self.path, self.localpath)
        if status:
            self.downloaded = True
            return True
        return False

    def init_db_connection(self):
        self.db = sqlite3.connect(self.localpath)

    def close_db(self):
        self.db.close()
        os.unlink(self.localpath)

    def get_key(self, key):
        query = 'select cpoint_id,key,flg,exp,cas,val,"%s" from cpoint_op where key="%s" and op="m"' %(self.date, key)
        cursor = self.db.execute(query)
        return cursor.fetchone()

    def validate_blob(self, key_item):
        return True

    def find_keys(self, keys, validate=False):
        output = {}

        for k in keys:
            self.log("Looking for key:%s" %k)
            rv = self.get_key(k)
            if rv:
                if validate:
                    if self.validate_blob(rv):
                        output[k] = rv
                        self.log("KEY: %s DATE:%s STATUS:FOUND VALIDATION:TRUE" %(k, self.date))
                    else:
                        self.log("KEY: %s DATE:%s STATUS:CORRUPT VALIDATION:TRUE" %(k, self.date))

                else:
                    output[k] = rv
                    self.log("KEY: %s DATE:%s STATUS:FOUND VALIDATION:FALSE" %(k, self.date))

        return output

    def __repr__(self):
        return 'backup:%s shard:%d path:%s' %(self.date, self.split_no, self.path)

class Job:
    """
    Job class
    """

    def __init__(self):
        self.config = BlobrestoreManager.configclass
        self.exit_status = 0
        self.buffers = Semaphore(self.config.download_buffers_per_job)
        self.download_queue = Queue.Queue()
        self.restore_queue = Queue.Queue()
        self.total_keys_count = 0
        self.keylist = []
        self.complete = False
        self.download_onprogress = False

    def parse_nodejob(self, i, nodejob, njt):
        self.nodejob_tracker = njt
        self.node_jobid = nodejob.job_id
        self.hostname = '%s-%03d' %(nodejob.hostname_prefix, i+1)
        self.game_id = nodejob.game_id
        self.validate_blob = nodejob.validate_blob
        self.restore_date = nodejob.restore_date
        self.force_find_days = nodejob.force_find_days
        self.check_master_backup = nodejob.check_master_backup
        self.keylist.extend(nodejob.jobs[i])
        self.total_keys_count = len(self.keylist)

    def activate(self):
        msg = "Starting blobrestore job #%s_ID%d hostname:%s" %(self.game_id,
            self.node_jobid, self.hostname)
        self.dirpath = os.path.join(consts.BLOBRESTORE_PROCESSED_JOBS_DIR,
        '%s_ID%d' %(self.game_id, self.node_jobid))
        try:
            os.system("rm -rf %s/*" %self.dirpath)
            os.makedirs(os.path.join(self.dirpath, 'output'))
        except:
            pass
        self.nodejob_tracker.activate()
        log(msg)
        self.log(msg)

    def log(self, msg):
        self.nodejob_tracker.log("%s : %s" %(self.hostname, msg))

    def process(self):
        try:
            bs = BackupServer(self.config.s3bucket, self.game_id, self.hostname)
            self.log("Searching for daily backups")
            backup_search_list = bs.list_backups(self.restore_date,
                    self.force_find_days, self.check_master_backup)
            if backup_search_list:
                if len(backup_search_list):
                    for b in backup_search_list:
                        self.log("Found backup %s" %b.path)
                        self.download_queue.put(b)
                else:
                    self.log("No backups found")
            else:
                self.log("Downloading backup list failed")
            dt = Thread(target=self.download_process)
            dt.daemon = True
            dt.start()

            rt = Thread(target=self.restore_process)
            rt.daemon = True
            rt.start()

            dt.join()
            rt.join()
        except Exception, e:
            self.log("Failed blobrestore process (ERROR: %s)" %str(e))
            self.exit_status = 1

        log("Completed blobrestore job #%s_ID%d hostname:%s" %(self.game_id,
            self.node_jobid, self.hostname))
        self.complete = True

    def remove_from_keylist(self, keys):
        for k in keys:
            self.keylist.remove(k)

    def download_process(self):
        try:
            self.log("Starting download process for Job #%s" %self.hostname)
            while len(self.download_queue.queue) and not self.exit_status:
                self.download_onprogress = True
                self.buffers.acquire()
                backup = self.download_queue.get()
                self.log("Attempt: Downloading backup : %s" %backup.path)
                if not backup.download_to_dir(os.path.join(self.dirpath,
                    'downloads/')):
                    self.log("Failed: Downloading backup : %s" %backup.path)
                    self.log("Failed download process for Job #%s" %self.hostname)
                    self.exit_status = 1
                    return
                self.log("Complete: Downloading backup : %s" %backup.path)
                self.restore_queue.put(backup)
                self.download_onprogress = False
            self.log("Completed download process for Job #%s" %self.hostname)
        except Exception, e:
            self.log("Failed download process (ERROR: %s)" %str(e))
            self.exit_status = 1

        
    def restore_process(self):
        try:
            keystore = KeyStore(os.path.join(self.dirpath, 'output',self.hostname))
            self.log("Starting restore process for Job #%s" %(self.hostname))
            while (len(self.download_queue.queue) or len(self.restore_queue.queue)
                    or self.download_onprogress) and len(self.keylist) and not self.exit_status:
                if len(self.restore_queue.queue):
                    backup = self.restore_queue.get()
                    backup.init_db_connection()
                    backup.setLogger(self.log)
                    found_keys = backup.find_keys(self.keylist)
                    self.remove_from_keylist(found_keys.keys())
                    keystore.write(found_keys)
                    backup.close_db()
                    self.buffers.release()
                else:
                    time.sleep(1)

            keystore.close()
            if not self.exit_status:
                self.log("Completed restore process for Job #%s" %(self.hostname))
        except Exception, e:
            self.log("Failed restore process (ERROR: %s)" %str(e))
            self.exit_status = 1


    def get_status(self):
        return self.total_keys_count, self.total_keys_count - len(self.keylist), len(self.keylist), self.exit_status

class BlobrestoreWorker(Thread):
    """
    Worker class that handles a blobrestore job
    """

    count = 0

    def __init__(self, global_job_queue, nodejob_status_queue=None):
        Thread.__init__(self)
        self.__class__.count +=1
        self.worker_id = self.__class__.count 
        log("Starting BlobrestoreWorker #%d" %self.worker_id)
        self.global_job_queue = global_job_queue
        self.current_job = None

    def run(self):
        while True:
            self.pick_job()
            self.process_job()
            time.sleep(1)

    def pick_job(self):
        self.current_job = self.global_job_queue.get()

    def process_job(self):
        self.current_job.activate()
        self.current_job.process()

class BlobrestoreManager:
    """
    Manager class that holds all the components of blobrestore job processing
    """
    configclass = None

    def __init__(self):
        self.job_queue = Queue.Queue()
        self.nodejob_tracker = NodeJobTracker.instances
        try:
            self.config = Config(consts.CONFIG_FILE)
            self.config.read()
            self.logger = Logger(tag = self.config.syslog_tag, level = self.config.log_level)
        except Exception, e:
            self.config.syslog_tag = consts.SYSLOG_TAG_BLOBRESTORE
            self.logger = Logger(tag = self.config.syslog_tag, level = self.config.log_level)
            self.logger.log("FAILED: Parsing config file (%s)" %(str(e)))
        self.__class__.configclass = self.config

        try:
            os.makedirs(os.path.join(consts.BLOBRESTORE_JOBS_DIR, 'pending'))
            os.makedirs(consts.BLOBRESTORE_PROCESSED_JOBS_DIR)
        except:
            pass

        for s in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(s, self.stop)

    def job_scanner_process(self):
        """
        Scanner that looks into jobs directory and add job in to jobs queue
        """

        while True:
            log("Scanning for restore jobs")
            job_files = os.listdir(consts.BLOBRESTORE_JOBS_DIR)
            job_files = filter(lambda x: x.endswith('.njob'), job_files)
            if len(job_files):
                log("Found jobs: %s" %", ".join(job_files))
            for nj in job_files:
                try:
                    jobpath = os.path.join(consts.BLOBRESTORE_JOBS_DIR, nj)
                    marker = os.path.join(consts.BLOBRESTORE_JOBS_DIR,
                            'pending', nj)
                    os.system("touch %s" %marker)
                    node_job = pickle.load(open(jobpath, 'rb'))
                    os.unlink(jobpath)
                    nodejob_tracker = create_nodejob_tracker(node_job)
                    for j in nodejob_tracker.jobs:
                        self.job_queue.put(j)
                        j.job_file_path = jobpath
                except Exception, e:
                    log("Error occured: %s" %str(e))
                    continue

            time.sleep(5)

    def restore_process(self):
        """
        Spawn work processes to process the blobrestore jobs
        """

        #for i in range(self.config.parallel_jobs):
        for i in range(2):
            bw = BlobrestoreWorker(self.job_queue)
            bw.start()

    def nodejob_status_process(self):
        """
        Update the status file of node job at intervals
        """
        while True:
            nodejobs = NodeJobTracker.instances
            for njt in nodejobs:
                if njt.activated:
                    nj_status = njt.get_status()
                    if nj_status[-1] > 0:
                        failure = True
                    else:
                        failure = False
                    nj_status = nj_status[:-1]
                    f = open(os.path.join(njt.dirpath, 'status'), 'w')
                    if njt.complete:
                        if failure:
                            f.write('%d %d %d failed' %nj_status)
                        else:
                            f.write('%d %d %d complete' %nj_status)

                        nodejobs.remove(njt)
                        del njt
                    else:
                        f.write('%d %d %d onprogress' %nj_status)

                    f.close()

            time.sleep(5)

    def clear_env(self):
        os.system("rm -rf %s/*" %consts.BLOBRESTORE_PROCESSED_JOBS_DIR)

    def start(self):
        """
        Start blobrestore Manager
        """

        log("=========Starting Blobrestore Manager=========")
        self.clear_env()

        rt = Thread(target=self.restore_process)
        rt.daemon = True
        rt.start()

        sct = Thread(target=self.job_scanner_process)
        sct.daemon = True
        sct.start()

        stt = Thread(target=self.nodejob_status_process)
        stt.daemon = True
        stt.start()

        while True:
            time.sleep(10)

    def stop(self, signum=None, frame=None):
        """
        Stop blobrestore manager gracefully
        """
        log("Writing queued jobs into jobs directory")
        for nj in filter(lambda x: x.activated==False, NodeJobTracker.instances):
            filename = "nodejob_%s_ID%d.njob" %(nj.nodejob_ref.game_id,
                    nj.nodejob_ref.job_id)
            log("Writing %s" %filename)
            f = open(os.path.join(consts.BLOBRESTORE_JOBS_DIR, filename), 'wb')
            pickle.dump(nj.nodejob_ref, f)
            f.close()
        sys.exit(0)

if __name__ == '__main__':
    bm = BlobrestoreManager()
    bm.start()

