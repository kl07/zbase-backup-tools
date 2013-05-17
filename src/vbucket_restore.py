#!/usr/bin/env python26

import sys, os
import thread, threading
from logger import Logger
from time import sleep
import pdb
import socket
from sendfile import sendfile
import commands
import string
from download_client import download_client
import consts
import json


class vbucketRestore:

    def __init__(self, diskmapper):

        self.diskmapper = diskmapper

    def get_storage_server(self, vb_id):

        map_available = False
        for i in range(consts.CONNECT_RETRIES):
            vb_query_cmd = "curl -s \"http://" + self.diskmapper + "/api?action=get_vb_mapping&&vbucket=vb_" + str(vb_id) + "\""
            status, output = commands.getstatusoutput(vb_query_cmd)

            print vb_query_cmd

            if status > 0:
                print "Failure: Unable to fetch disk mapping. Command %s output %s" %(fetch_map_cmd, output)
                if i >= consts.CONNECT_RETRIES:
                    break
                continue
            elif status == 0:
                try:
                    current_map = json.loads(output)
                    map_available = True
                    storage_server = current_map['storage_server']
                    break
                except Exception, e:
                    print"Unable to parse output %s" %str(e)
                    break

        if map_available == False:
            # Failure no map available :(
            return ""

        print "Storage server %s" %storage_server
        return storage_server


    def get_checkpoints(self, vb_list):

        checkpoint_list = {}
        try:
            for vb_id in vb_list:
                #look up storage server for vb_id
                storage_server = self.get_storage_server(vb_id)
                if storage_server == "":
                    print ("Failed to get storage server for vb_id %d" %vb_id)
                    checkpoint_list[vb_id] = -1
                    continue
                #get the last checkpoint id for this vb
                client = download_client(storage_server, consts.SS_PORT)
                status, checkpoint = client.get_checkpoint(vb_id)

                if status == -1 or checkpoint == None:
                    print ("Failed to get checkpoint id for vb_id %d" %vb_id)
                    checkpoint_list[vb_id] = -1
                    continue

                checkpoint_list[vb_id] = int(checkpoint)

        except Exception, e:
            print "get checkpoints failed with error %s " %str(e)
            return False, checkpoint_list

        return True, checkpoint_list

    def restore_vbuckets(self, vb_list):

        output_status = []
        for vb_id in vb_list:
            restore_cmd = "python26 " + consts.RESTORE_CMD + " -v " + str(vb_id) + " -d " + self.diskmapper
            print "Executing command %s" %restore_cmd
            status, output = commands.getstatusoutput(restore_cmd)
            restore_status = {}
            restore_status['vb_id'] = vb_id
            restore_status['status'] = status

            if status != 0:
                print ("Restore for vb_id %d failed. Output: %s" %(vb_id, output))
                restore_status['output'] = output
            else:
                print ("Restore for vb_id %d successful" %vb_id)
                restore_status['status'] = "Restore successful"

            output_status.append(restore_status)

        return output_status


if __name__ == '__main__':

    #unit test
    disk_mapper = "172.21.13.72"
    vb_list = [25, 26, 27]

    vb_restore = vbucketRestore(disk_mapper)
    status, checkpoint_list = vb_restore.get_checkpoints(vb_list)

    if status == True:
        print checkpoint_list
    else:
        print "Checkpoint list failed :("

    output = vb_restore.restore_vbuckets(vb_list)
    print output





