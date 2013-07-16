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

import sys, os
import thread, threading
from logger import Logger
from time import sleep
import pdb
sys.path.append('./vbs_agent/')
import vbs_agent
import json
import socket
import commands
import util
from util import getcommandoutput

#globals

doneEvent = threading.Event()
doneEvent.clear()

mapLock = threading.Lock()
currentVbsMap = {}

class InitBackupDaemon:

    """
        initialize the vbucketdaemon.
        1. connect to the disk-mapper and get the list of vbuckets belonging
        to this server
        2. initialize connection to the VBS and maintain an updated mapping of
        vbuckets to the server

    """

    def __init__(self, dm_host, vbs_host):

        self.mapping_ready = 0
        self.dm_host = dm_host
        self.ip_address = socket.gethostbyname(socket.gethostname())

        """
            This thread connects to the VBS server and maintains
            the updated vbucket map

        """

        self.logger = Logger("vBucketBackupd", "INFO")
        v_thread = vbs_thread(vbs_host, self.logger)
        v_thread.start()

        # wait till the vbs array has been populated before returning
        doneEvent.wait()

        if self.get_disk_mapping() == {}:
            self.logger.log("Fatal: Could not get mapping from disk mapper ")
            return

        self.mapping_ready = 1
        return


    ## connect to the disk mapper and
    def get_disk_mapping(self):

        ##connect to the disk mapper and get a list of vb_ids and associated pathnames
        map_available = False
        for i in range(5):
            fetch_map_cmd = "curl \'http://" + self.dm_host + "/api?action=get_ss_mapping&storage_server=" + self.ip_address + "\'" + " -o /tmp/currentmap"
            status,output = util.getcommandoutput(fetch_map_cmd)

            if status > 0:
                self.logger.log("Failure: Unable to fetch disk mapping. Command %s output %s" %(fetch_map_cmd, output))
                if i >= 5:
                    break
                continue
            elif status == 0:
                try:
                    map = open("/tmp/currentmap", "r")
                    buffer = map.read()
                    current_map = json.loads(buffer)
                    map.close()
                    map_available = True
                    break
                except Exception, e:
                    self.logger.log("Unable to read file %s" %str(e))
                    break


        if map_available == False:
            # Failure no map available :(
            return {}

        dm_map = {}
        for key in current_map.keys():
            discard, vb = key.split("_")
            vb_id = int(vb)
            dm_map[vb_id] = current_map[key]['path_name']

        self.logger.log("Info: disk map initialized")
        return dm_map

    def generate_disk_map (self):

        if self.mapping_ready == 0:
            self.logger.log(" Warning: VBS to disk map cannot be generated ")
            return None

        dm_map = self.get_disk_mapping()
        if len(dm_map) == 0:
            self.logger.log(" Fatal: Failed to get disk mapping ")
            return None

        vb_disk_map = []
        mapLock.acquire()

        for vb_id in dm_map.keys():
            global currentVbsMap
            try:
                vb_record = {}
                vb_record['vb_id'] = vb_id
                vb_record['path'] = dm_map[vb_id]
                vb_record['server'] = currentVbsMap[vb_id]
                vb_disk_map.append(vb_record)
            except:
                continue

        mapLock.release()
        return vb_disk_map


    def get_current_mapping (self):

        if self.mapping_ready == 0:
            return None

        elif self.mapping_ready == 1:
            ## wait for lock
            vb_disk_map = generate_disk_map()
            return vbs_disk_map


class vbs_thread(threading.Thread):

    def __init__ (self, vbs_host, logger):

        self.vbs_host = vbs_host
        self.logger = logger
        self.thread_id = "VBS thread"
        self.vbs_map_ready = 0
        threading.Thread.__init__(self)

    def run(self):

        self.logger.log("Starting thread %s" %self.thread_id)
        ## connect to the vbs server and create populate the vb_array
        ## that contains the mapping between vb_id belonging to this server
        ## and the ip address of the membase server on which that vbucket
        ## resides
        try:
            host, port = self.vbs_host.split(':')
        except:
            host = self.vbs_host
            port = 14000 #default VBS port

        vbs_agent.vbs_config_ptr = vbs_agent.create_vbs_config(host, int(port))
        vbs_start_status = vbs_agent.start_vbs_config(vbs_agent.vbs_config_ptr)

        if vbs_start_status == -1:
            self.logger.log("Fatal: Failed to connect to VBS server %s" %vbs_host)
            sys.exit(-1)

        while (1):
            sleep(5)
            #dummy stuff TODO
            vbs_map_string = vbs_agent.get_current_config()
            if vbs_map_string == None:
                self.logger.log("Failure: Cannot get vbucket map")
                continue

            try:
                vbs_map_string = json.loads(vbs_map_string)
                server_config = vbs_map_string[0]['vBucketServerMap']
                vb_list = server_config['vBucketMap']
                server_list = server_config['serverList']
            except:
                self.logger.log("Failure: Unable to parse vbucket map")

            vb_id = 0
            vbs_map = {}
            for server_id in vb_list:
                # need to connect to the replica vbucket
                index = 0
                if len(server_id) > 1:
                    index = 1

                serv_index = server_id[index]
                # Skip vbuckets or servers in maintenance stages
                if serv_index >= 0 and server_list[serv_index] != "0.0.0.0":
                    vbs_map[vb_id] = server_list[serv_index]

                vb_id = vb_id + 1

            self.update_map(vbs_map)

            if self.vbs_map_ready == 0:
                doneEvent.set()
                doneEvent.clear()
                self.vbs_map_ready = 1


    def update_map(self, vbs_map):

        mapLock.acquire(True)
        global currentVbsMap
        currentVbsMap = vbs_map
        mapLock.release()

