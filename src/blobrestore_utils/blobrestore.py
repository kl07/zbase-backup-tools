#!/usr/bin/env python26
#Description: Blob restore driver for Membase 1.7.3

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

import sys
import zlib
import getopt
import random
import pickle
import os
import signal
import logging
import tempfile
import sqlite3
import datetime
import calendar
import time
import json
import socket

PYTHON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
SSH_KEY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blobrestore_sshkey')
sys.path.insert(0, PYTHON_PATH)

import consts
from mc_bin_client import MemcachedClient
from util import getcommandoutput, zruntime_readkey
from config import Config

def exit(*x):
    print "Exiting the blobrestore driver"
    sys.exit(1)

def init_logger():
    global logger
    logfile = 'blobrestore.log'
    logger = logging.getLogger('Blobrestore')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler_file = logging.FileHandler(logfile)
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

def log(msg):
    global logger
    print msg
    logger.info(msg)

def download_file(s3path, localpath):
    dl_cmd = "%s get %s %s" %(consts.PATH_S3CMD_ZYNGA_EXEC, s3path, localpath) 
    status, output = getcommandoutput(dl_cmd)
    if status == 0:
        try:
            if 'Error' in open(localpath).read():
                status = 1
        except Exception, e:
            status = 2
    else:
        status = 2

    return status

def usage(e=0):
    """
    Print usage
    """

    print "\nUsage: %s addjob -k keylist_file -t '2012-01-02 07:00:03'" \
            " -d disk_mapper_server [ -f force_find_days ]" \
            " [ -c ] [ -m ]\n" %(sys.argv[0])
    print "%s status restore_ID.job\n" %(sys.argv[0])
    print "%s fetchlog restore_ID.job\n" %(sys.argv[0])
    print "%s restore-server restore_ID.job [-v vbs_server | -s server ] [ -r ]\n" %(sys.argv[0])

    sys.exit(e)

def keyhash(key):
    """
    Return crc hash for the key
    """
    crchash = (zlib.crc32(key) >> 16) & 0x7fff
    if crchash:
        return crchash
    else:
        return 1

def get_vbucket(key, nvbuckets):
    """
    Find out the vbucket_id from key
    """
    return keyhash(key) & (nvbuckets-1)

def group_keys(key_file, nvbuckets):
    """
    Group keys into vbucket groups
    """
    groups = {}
    try:
        handle = open(key_file, 'r')
    except:
        sys.exit("Invalid key list file")

    for key in handle:
        key = key.strip()
        if key =='':
            continue
        vb = "vb_%d" %get_vbucket(key, nvbuckets)
        if not groups.has_key(vb):
            groups[vb] = []
        groups[vb].append(key)

    if not len(groups.keys()):
        sys.exit("No keys found in the keylist file")

    return groups

def remote_filecopy(src_file, dest_file):
    if ':' in src_file:
        src_file = "storageserver@%s" %src_file

    if ':' in dest_file:
        dest_file = "storageserver@%s" %dest_file

    cmd = "scp -i %s -r -q -o PasswordAuthentication=no -o" \
            "StrictHostKeyChecking=no %s %s" %(SSH_KEY_PATH, src_file, dest_file)
    status = os.system(cmd)
    if not status:
        return True
    else:
        return False

def remote_cmd(server, cmd):
    cmd = 'ssh -i %s -o PasswordAuthentication=no -o StrictHostKeyChecking=no storageserver@%s "%s"' \
            %(SSH_KEY_PATH, server, cmd)

    return getcommandoutput(cmd)

def get_storageserver_map(mapping_server):
    """
    Fetch the server to storage server mapping
    from mapping server API
    """
    cmd = "curl -s -L http://%s/%s" %(mapping_server, consts.BLOBRESTORE_API_PATH)
    status, output = getcommandoutput(cmd)
    if status == 0:
        try:
            return json.loads(str(output))
        except Exception, e:
            sys.exit("ERROR: Unable to parse host to storage server map (%s)" \
                    %str(e))
    else:
        sys.exit("ERROR: Failed to download host to storage server map from server:%s" \
                %(mapping_server))

def parse_args(args):
    """
    Parse the command-line arguments into a class
    """

    if len(args) < 3:
        usage("ERROR: Not enough arguments")

    config = Config(consts.CONFIG_FILE)
    config.read()

    options = {}
    options['command'] = args[1]

    if options['command'] not in ['addjob' ,'status', 'fetchlog', 'restore-server']:
        usage("ERROR: wrong command or command not specified")

    if options['command'] == 'addjob':
        if len(args) < 8:
            usage("ERROR: Not enough arguments")

        options['validate_blob'] = False
        options['check_master_backup'] = False
        try:
            opts, args = getopt.getopt(args[2:], 'k:n:t:d:p:f:cmh')
        except getopt.GetoptError, e:
            usage(e.msg)

        try:
            for (o,a) in opts:
                if o == '-k':
                    options['key_file'] = a
                elif o == '-t':
                    options['restore_date'] = a
                    try:
                        now = time.time()
                        if " " in a:
                            d,t = a.split(' ')
                            year, month, day = map(lambda x: int(x), d.split('-'))
                            hr, mins, secs = map(lambda x: int(x), t.split(':'))
                            if (month not in range(1,13)) or \
                                (day not in range(1, 32)) or \
                                  (hr not in range(0,24)) or \
                               (mins not in range(0, 61)) or \
                                (secs not in range(0,61)):
                                    raise Exception
                            restore_datetime = calendar.timegm((year,month,day,hr,mins,secs))
                            if restore_datetime < (now - 24*60*60):
                                log("Warning: The time (%s) is ignored since date is older than 1 day" %t)
                        else:
                            year, month, day = map(lambda x: int(x), a.split('-'))
                            restore_datetime = calendar.timegm((year,month,day,0,0,0))

                        if restore_datetime > now:
                            usage("ERROR: Trying to restore from future date")
                    except Exception, e:
                        usage("ERROR: Invalid date specified")

                elif o == '-d':
                    options['disk_mapper_server'] = a

                    if options['disk_mapper_server'][0].isdigit():
                        options['game_id'] = options['disk_mapper_server']
                    else:
                        options['game_id'] = options['disk_mapper_server'].split('.')[0]

                elif o == '-f':
                    options['force_find_days'] = int(a)
                elif o == '-c':
                    options['validate_blob'] = True
                elif o == '-m':
                    options['check_master_backup'] = True
                elif o == '-h':
                    usage()
        except Exception, e:
            usage("Invalid arguments (%s)" %str(e))

    elif options['command'] == 'restore-server':
        if len(args) < 4:
            usage("ERROR: Not enough arguments")

        options['job_config'] = args[2]
        options['repair_mode'] = False
        try:
            opts, args = getopt.getopt(args[3:], 's:v:r')
        except getopt.GetoptError, e:
            usage(e.msg)


        for (o,a) in opts:
            if o == '-v':
                if ':' not in a:
                    a = "%s:14000" %a
                options['vbs_server'] = a
            elif o == '-r':
                options['repair_mode'] = True
            elif o == '-s':
                options['server_addr'] = a

    else:
        options['job_config'] = args[2]

    return options

class MembasePool:
    def __init__(self):
        self.servers = []

    def addServer(self, ipaddr):
        try:
            mc = MemcachedClient(host=ipaddr, port=11211)
            self.servers.append(mc)
            return True
        except Exception,e:
            log("Unable to add server %s (%s)" %(ipaddr, str(e)))
            sys.exit(1)
            return False

    def set_key(self, vbid, key, flg, exp, val, date, cksum):
        server = None
        try:
            count = len(self.servers)
            if count == 0:
                log("No servers added to server list")
                sys.exit(1)
            server = self.servers[keyhash(key) % count]
            if not server.options_supported():
                cksum = None
            server.set(key, exp, flg, val, cksum)
            log("Successfully set vbid:%d key:%s to server:%s date:%s" %(vbid, key,
                server.host, date))
        except Exception, e:
            if server:
                log("Unable to set key:%s to server:%s (%s)" %(key,
                    server.host, str(e)))
            else:
                log("Unable to set key:%s to server (%s)" %(key, str(e)))

class VBCluster:
    def __init__(self, vbs_server):
        cmd = "curl -s -L http://%s/%s" %(vbs_server, consts.VBS_API_PATH)
        status, output = getcommandoutput(cmd)
        if status > 0:
            log("ERROR: Unable to contact VBS: %s" %(vbs_server))
            sys.exit(1)

        j = json.loads(output)
        vbsmap = j['buckets'][0]['vBucketServerMap']['vBucketMap']
        srvlist = j['buckets'][0]['vBucketServerMap']['serverList']
        self.vbmap = {}

        for i,v in enumerate(vbsmap):
            ip, port = srvlist[v[0]].split(':')
            mc = MemcachedClient(host=ip, port=int(port))
            self.vbmap[i] = mc

    def set_key(self, vbid, key, flg, exp, val, date, cksum):
        try:
            server = self.vbmap[vbid]
            server.vbucketId = vbid
            server.set(key, exp, flg, val, cksum)
            log("Successfully set vbid:%d key:%s to server:%s date:%s" %(vbid, key,
                server.host, date))
        except Exception, e:
            if server:
                log("Unable to set key:%s to server:%s (%s)" %(key,
                    server.host, str(e)))
            else:
                log("Unable to set key:%s to server (%s)" %(key, str(e)))

class KeyStore:
    """
    Class for writing restored KeyStore
    """

    def __init__(self, filepath):
        self.filepath = filepath
        self.db = sqlite3.connect(self.filepath)
        self.read_cursor = None
        self.db.executescript("""
        BEGIN;
        CREATE TABLE IF NOT EXISTS restored_keys
        (cpoint_id integer, key varchar(250), flg integer, exp integer, cas integer, val blob, date text, cksum varchar(100));
        COMMIT;
        """)

    def write(self, blobs):
        for b in blobs.values():
            if b[1]:
                c = self.db.execute('INSERT INTO restored_keys VALUES(?,?,?,?,?,?,?,?);', b[0])
            else:
                c = self.db.execute('INSERT INTO restored_keys(cpoint_id,key,flg,exp,cas,val,date) VALUES(?,?,?,?,?,?,?);', b[0])

    def get_read_cursor(self):
        cursor = self.db.execute('SELECT key,flg,exp,val,date,cpoint_id,cksum from restored_keys')
        return cursor

    def read(self):
        if not self.read_cursor:
            self.read_cursor = self.get_read_cursor()

        return self.read_cursor.fetchone()

    def close(self):
        self.db.commit()
        self.db.close()

class NodeJob:
    """
    Job config class for individual array nodes
    """

    def __init__(self, options, ipaddr):
        self.node_job_file = None
        self.ipaddr = ipaddr
        self.job_id = options['job_id']
        self.game_id = options['game_id']
        self.validate_blob = options['validate_blob']
        self.restore_date = options['restore_date']
        self.force_find_days = 0
        if options.has_key('force_find_days'):
            self.force_find_days = options['force_find_days']
        self.check_master_backup = False
        if options.has_key('check_master_backup'):
            self.check_master_backup = options['check_master_backup']

        self.jobs = {}

    def add_bucketdata(self, shard, keys, disk):
        self.jobs[shard] = keys, disk

    def write_to_file(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        pickle.dump(self, f)
        f.close()
        self.node_job_file = f.name

    def push_to_node(self):
        log("Pushing nodejob to %s" %self.ipaddr)
        job_file_path = \
        os.path.join(consts.BLOBRESTORE_JOBS_DIR,'nodejob_%s_ID%d.njob'
                %(self.game_id, self.job_id))
        status = remote_filecopy(self.node_job_file,
                "%s:%s" %(self.ipaddr, job_file_path))
        if status:
            os.unlink(self.node_job_file)
        return status

    def get_jobstatus(self):
        job_file = 'nodejob_%s_ID%d.njob' %(self.game_id, self.job_id)
        status_dict = {'job_exist':True, 'restored':0, 'remaining':0}

        status, output = remote_cmd(self.ipaddr, 'find %s -type f -name %s'
                %(consts.BLOBRESTORE_JOBS_DIR, job_file))
        if status == 0:
            if output.strip() != '':
                status_text = '%s: Job not scheduled' %(self.ipaddr)
            else:

                status, output = remote_cmd(self.ipaddr, 'cat %s/%s/status'
                                %(consts.BLOBRESTORE_PROCESSED_JOBS_DIR,
                                    "%s_ID%d" %(self.game_id, self.job_id)))
                if status == 0:
                    if output.strip() != '':
                        total, restored, remaining, status = output.strip().split(' ')
                        status_dict['total'] = int(total)
                        status_dict['restored'] = int(restored)
                        status_dict['remaining'] = int(remaining)
                        status_dict['status'] = status
                        status_text = '%s: Job status: %s (%s/%s)' %(self.ipaddr, status, restored, total)
                else:
                    status_text = '%s: Job not found in this server' %self.ipaddr
                    status_dict['job_exist'] = False
        else:
            status_text = "%s : Connection to server failed" %(self.ipaddr)
            status_dict['job_exist'] = False

        log(status_text)
        return status_dict

    def get_log(self):
        status, output = remote_cmd(self.ipaddr, 'cat %s/%s/restore.log'
                    %(consts.BLOBRESTORE_PROCESSED_JOBS_DIR,
                    "%s_ID%d" %(self.game_id, self.job_id)))
        print output

    def download_restored_keys(self, dirpath):
        jobdir = '%s_ID%d' %(self.game_id, self.job_id)
        src_path = "%s:%s/*" %(self.ipaddr, os.path.join(consts.BLOBRESTORE_PROCESSED_JOBS_DIR, jobdir, 'output'))
        log("\t* Downloading from node:%s" %self.ipaddr)
        status = remote_filecopy(src_path, dirpath)
        if not status:
            log("%s: Downloading restored keys failed" %self.ipaddr)

class BlobrestoreDispatcher:
    """
    The blobrestore driver class
    """

    def __init__(self, options):
        self.node_job = {}
        command = options['command']
        self.options = options
        if command == 'addjob':
            self.do_addjob()
        elif command == 'status':
            self.do_status()
        elif command == 'fetchlog':
            self.do_fetchlog()
        elif command == 'restore-server':
            self.do_server_restore()

    def exit(self, msg):
        log(msg)
        sys.exit(1)

    def _create_job_fromoptions(self, options):
        """
        Create jobs from options
        """

        job_id = random.randint(0,10000000000)
        self.options['job_id'] = job_id
        storageserver_map = get_storageserver_map(self.options['disk_mapper_server'])
        grouped_keys = group_keys(self.options['key_file'], len(storageserver_map))
        for vb in grouped_keys:
            if storageserver_map.has_key(vb):
                data = storageserver_map[vb]
            else:
                sys.exit("ERROR: Unable to find %s in storage server map" %vb)

            if self.node_job.has_key(vb):
                self.node_job[data['storage_server']].add_bucketdata(vb,
                        grouped_keys[vb], data['disk'])
            else:
                ipaddr = data['storage_server']
                job = NodeJob(self.options, ipaddr)
                job.add_bucketdata(vb, grouped_keys[vb], data['disk'])
                self.node_job[ipaddr] = job

    def _create_batchjob_file(self):
        """
        Create output .job file
        """

        filename = "%s_restore_ID%d.job" %(self.options['game_id'],
                self.options['job_id'])
        try:
            f = open(filename, 'wb')
            pickle.dump(self.node_job, f)
            f.close()
        except:
            sys.exit("Unable to write file %s" %filename)
        return filename

    def _load_job_file(self):
        """
        Load job information from config file
        """
        filename = self.options['job_config']
        try:
            f = open(filename, 'rb')
            self.node_job = pickle.load(f)
            f.close()
        except:
            sys.exit("Invalid job file %s" %filename)

        entries = self.node_job.keys()
        if not len(entries):
            sys.exit("Nodejobs not found in job file")

        self.job_id = self.node_job[entries[0]].job_id
        self.options['game_id'] = self.node_job[entries[0]].game_id
        self.options['job_id'] = self.node_job[entries[0]].job_id

    def do_addjob(self):
        """
        Create restore job and send themG to the array nodes
        """
        self._create_job_fromoptions(self.options)
        for j in self.node_job.values():
            j.write_to_file()
            rv = j.push_to_node()
            if not rv:
                log("FAILED: Pushing job to node:%s failed" %j.ipaddr)
        batch_jobfile = self._create_batchjob_file()
        log("Created Blobrestore job - Job reference file is %s" %batch_jobfile)

    def do_status(self, reschedule=True):
        """
        Query the status of jobs from array nodes
        """

        reschedule_jobs = []

        status_dict = {'total':0, 'restored':0, 'remaining':0, 'status':'complete'}
        self._load_job_file()
        for node_job_item in self.node_job.values():
            status = node_job_item.get_jobstatus()
            if status['job_exist']:
                if status.has_key('total'):
                    status_dict['total'] += status['total']
                    status_dict['restored'] += status['restored']
                    status_dict['remaining'] += status['remaining']
                    if status['status'] != 'complete':
                        status_dict['status'] = 'in-progress'
                else:
                    status_dict['status'] = 'Not all nodejobs scheduled'
            else:
                status_dict['status']  = 'Nodejobs broken'
                reschedule_jobs.append(node_job_item)

        log("SUMMARY # TOTAL:%d RESTORED:%d REMAINING:%d " \
                "STATUS:%s" %(status_dict['total'], status_dict['restored'],
                    status_dict['remaining'], status_dict['status'])) 
        if reschedule and len(reschedule_jobs):
            yesno = raw_input("Do you want to reschedule/retry notfound jobs "
                    "(y/n)? ")
            if yesno == 'y':

                for nodejob in reschedule_jobs:
                    nodejob.write_to_file()
                    if not nodejob.push_to_node():
                        sys.exit(1)
                self._create_batchjob_file()
                log("Rescheduling jobs completed successfully")
        else:
            return status_dict

    def do_fetchlog(self):
        """
        Fetch the logs from the array nodes and consolidate
        """
        self._load_job_file()
        for node_job_item in self.node_job.values():
            node_job_item.get_log()

    def do_server_restore(self):
        """
        Perform actual restore on servers by using restored keys
        """

        status = self.do_status(False)
        if status['status'] != 'complete':
            log("The restore nodejob processes are not complete")
            sys.exit(1)

        log("Creating temporary directory tmp-blobrestore")
        tmpdir = 'tmp-blobrestore'
        os.system('rm -rf %s' %tmpdir)
        os.mkdir(tmpdir)
        self._load_job_file()
        log("Downloading restored keys from blobrestore nodes")
        for node_job_item in self.node_job.values():
            node_job_item.download_restored_keys(tmpdir)

        if self.options.has_key('server_addr'):
            membasepool = MembasePool()
            membasepool.addServer(options['server_addr'])
        else:
            vbs_server = "%s.%s" %(self.options['vbs_server'])
            membasepool = VBCluster(vbs_server)

        for f in os.listdir(tmpdir):
            log("Reading keys from vbucket %s" %f)
            vbid = int(f.split('_')[1])
            fpath = os.path.join(tmpdir, f)
            ks = KeyStore(fpath)
            restored_key = ks.read()
            while restored_key:
                key, flg, exp, val, date, cpoint_id, cksum = restored_key
                if cksum:
                    cksum = str(cksum)
                if self.options['repair_mode']:
                    key += '_r'
                membasepool.set_key(vbid, str(key), socket.ntohl(flg), exp, str(val), date, cksum)
                restored_key = ks.read()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit)
    if os.getuid() != 0:
        print "Please run as root"
        sys.exit(1)

    init_logger()
    options = parse_args(sys.argv)
    bd = BlobrestoreDispatcher(options)

