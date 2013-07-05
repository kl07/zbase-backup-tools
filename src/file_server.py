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
import socket
from sendfile import sendfile
import commands
import json
import consts
import util
#from util import pause_coalscer, resume_coalescer

#globals


DEFAULT_SERVER_PORT = 22122

#Error messages

INVALID_SYNTAX = "-1\r\nInvalid Syntax\r\n"
INVALID_COMMAND = "-1\r\nInvalid Command\r\n"
ENOEXIST = "-1\r\nFile not found\r\n"
INVALID_TYPE = "-1\r\nInvalid type\r\n"
INTERROR = "-1\r\nInternal Error\r\n"
ISDIR = "-1\r\nRequested download is a directory\r\n"


class FileServer:

    def __init__(self, disk_mapper, host=None, port=None):


        self.disk_mapper = disk_mapper

        if host != None:
            self.host = host
        else:
            self.host = '0.0.0.0'

        if port != None:
            self.port = port
        else:
            self.port = 22122

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.host, self.port)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(server_address)
        self.logger = Logger("RestoreDaemon", "INFO")
        self.logger.log("Info: ===== Starting restore daemon ====== ")

    def request_thread(self, connection, client_address):

        while 1:
            data = connection.recv(4096)
            if data == '':
                print ("Info: client closed connection")
                connection.close()
                return

            data = data.rstrip()
            try:
                response_len = self.handle_cmd(connection, data)
            except Exception, e:
                connection.send("-1\r\n" + str(e) + "\r\n")
                continue

            if response_len == -1:
                connection.close()
                return


    def query_dm(self, vb_id):

        #query the disk_mapper and get the disk path
        if vb_id == "":
            vb_id = "0"
        vb_query_cmd = "curl -s \"http://" + self.disk_mapper + "/api?action=get_vb_mapping&&vbucket=vb_" + vb_id + "\""
        status, output = commands.getstatusoutput(vb_query_cmd)

        if status > 0:
            print "Failed to execute command %s. Output %s" %(vb_query_cmd, output)
            return None

        try:
            response = json.loads(output)
            return response
        except Exception, e:
            print >> sys.stderr, " Could not json parse output", str(e)
            return None

    def get_disk_path(self, vb_id):

        vb_id = vb_id.lstrip('0')
        if vb_id == "":
            vb_id = '0'
        response = self.query_dm(vb_id)
        if response == None:
            return None

        disk_path = "/"+ response["disk"] + "/" + response["type"] + "/" + response["vb_group"] + "/" + "vb_" + vb_id + "/"
        return disk_path

    def get_disk_basepath(self, vb_id):

        vb_id = vb_id.lstrip('0')
        if vb_id == "":
            vb_id = '0'
        response = self.query_dm(vb_id)
        if response == None:
            return None

        disk_path = "/"+ response["disk"]
        return disk_path


    def start(self):

        self.sock.listen(1)

        while True:
            connection, client_address = self.sock.accept()
            print >>sys.stderr, 'connection from', client_address
            thread.start_new_thread(self.request_thread, (connection, client_address))



    #LIST vb_id filename
    def handle_download(self, connection=None, data=None, checkpoint_only=False):

        data = data.split()

        if checkpoint_only == False:
            if len(data) != 3:
                return connection.send(INVALID_SYNTAX)
            filename = data[2]
        else:
            if len(data) != 2:
                return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)

        base_path = self.get_disk_path(vb_id)
        if base_path == None:
            return connection.send(INTERROR)

        if checkpoint_only == True:
            vb_id = vb_id.lstrip('0')
            if vb_id == "":
                vb_id = '0'
            file_path = base_path + "/vbid_" + vb_id + "_" + consts.LAST_CHECKPOINT_FILE
        else:
            file_path = base_path + filename

        if os.path.exists(file_path) == False:
            return connection.send(ENOEXIST)

        if os.path.isdir(file_path) == True:
            return connection.send(ISDIR)

        print ("info sending file %s" %file_path)
        file = open(file_path, "rb")
        size = os.stat(file_path).st_size
        response_line = str(size) + '\r\n'
        sent = connection.send(response_line)
        sent += sendfile(connection.fileno(), file.fileno(), 0, size)
        sent += connection.send('\r\n')
        file.close()

        return sent

    def handle_lock(self, connection=None, data=None):

        data = data.split()
        if len(data) != 3:
            return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)
        filename = data[2]

        base_path = self.get_disk_path(vb_id)
        if base_path == None:
            return connection.send(INTERROR)

        file_path = base_path + filename

        if os.path.exists(file_path) == True:
            return connection.send(ENOEXIST)


        print (" adding lock %s" %file_path)
        file = open(file_path, "w+")
        size = os.stat(file_path).st_size
        sent = connection.send("0\r\n")
        file.close()

        return sent

    def handle_pause(self, connection=None, data=None):

        data = data.split()
        if len(data) != 2:
            return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)
        base_path = self.get_disk_basepath(vb_id)
        if base_path == None:
            return connection.send(INTERROR)

        util.pause_coalescer(self.logger, base_path)
        sent = connection.send("0\r\n")
        return sent

    def handle_resume(self, connection=None, data=None):

        data = data.split()
        if len(data) != 2:
            return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)

        base_path = self.get_disk_basepath(vb_id)
        if base_path == None:
            return connection.send(INTERROR)

        util.resume_coalescer(self.logger, base_path)
        sent = connection.send("0\r\n")
        return sent

    #LIST vb_id volume type date_string
    def handle_list(self, connection=None, data=None):

        vb_id = 0
        data = data.split()

        if len(data) != 2 and len(data) != 3:
            return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)

        list_path = self.get_disk_path(vb_id)
        if list_path == None:
            return connection.send(INTERROR)

        if len(data) == 3:
            extra = data[2].rstrip('/')
            list_path = list_path + extra + '/'

        try:
            output = os.listdir(list_path)
        except Exception, e:
            print ("Problem happen ", str(e))
            return connection.send(ENOEXIST)


        ##format output
        formatted_output = ""
        for entry in output:
            if len(data) == 3:
                formatted_output += (extra + '/' + entry + "\r\n")
            else:
                formatted_output += (entry + "\r\n")

        return self.send_data(connection, formatted_output)


    def send_data(self, connection=None, output=None):

        if connection == None:
            return -1

        response_line = str(len(output)) + "\r\n"
        try:
            return_len = connection.send(response_line)
            return_len += connection.send(output)
            #return_len += connection.send("\r\n")
        except e, Exception:
            print >>sys.stderr, 'sending failed : ', str(e)

        return return_len


    def handle_remove(self, connection=None, data=None):

        data = data.split()
        if len(data) != 3:
            return connection.send(INVALID_SYNTAX)

        vb_id = str(data[1]).zfill(2)
        filename = data[2]

        base_path = self.get_disk_path(vb_id)
        if base_path == None:
            return connection.send(INTERROR)

        file_path = base_path + filename

        if os.path.exists(file_path) == False:
            return connection.send(ENOEXIST)

        delete_cmd = "rm -f " + file_path
        print ("Executing command %s" %delete_cmd)

        status,output = commands.getstatusoutput(delete_cmd)

        if status != 0:
            print ("Delete failed with reason %s" %output)
            return connection.send("-1\r\n" + output + "\r\n")

        # success
        return connection.send("0\r\n")


    def handle_cmd(self, connection=None, data=None):

        if data == None:
            return -1
        elif "LIST" in data:
            return self.handle_list(connection, data)
        elif "REMOVE" in data:
            return self.handle_remove(connection, data)
        elif "DOWNLOAD" in data:
            return self.handle_download(connection, data)
        elif "ADDLOCK" in data:
            return self.handle_lock(connection, data)
        elif "GETCHECKPOINT" in data:
            return self.handle_download(connection, data, True)
        elif "PAUSECOALESCER" in data:
            return self.handle_pause(connection, data)
        elif "RESUMECOALESCER" in data:
            return self.handle_resume(connection, data)
        else:
            return connection.send(INVALID_COMMAND)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print ("Usage file_server.py disk_mapper")
        sys.exit(1)
    else:
        disk_mapper = sys.argv[1]

    server_instance = FileServer(disk_mapper, "0.0.0.0", 22122)
    server_instance.start()


