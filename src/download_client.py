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

#globals


DEFAULT_SERVER_PORT = 22122

#Error messages

INVALID_SYNTAX = "-1\r\nInvalid Syntax\r\n"
INVALID_COMMAND = "-1\r\nInvalid Command\r\n"
ENOEXIST = "-1\r\nFile not found\r\n"
INVALID_TYPE = "-1\r\nInvalid type\r\n"

CRLF = "\r\n"

class download_client:

    def __init__(self, host, port):

        self.server_address = (host, port)
        self.is_connected = False


    def connect(self):

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(self.server_address)
        except Exception, e:
            print >> sys.stderr, "Cannot connect to" , self.server_address, str(e)
            return False

        return True

    def read_data(self):
        expected = -1
        received = 0
        buffer = ""

        try:
            while True:
                data = self.sock.recv(65535)
                if len(data) == 0:
                    # connection close
                    return "Error"
                if expected == -1:
                    offset = string.index(data, "\r\n")
                    resp_line = data[:offset]
                    expected = int(resp_line)
                    data = data[offset + len(CRLF):]

                if expected == -1:
                    return buffer

                if expected == 0:
                    return "Success"

                received += len(data)
                buffer += data

                if received >= expected:
                    break

            if buffer[-2:] == CRLF:
                buffer = buffer[:-2]
            elif buffer[-1:] == CRLF:
                buffer = buffer[:-1]

        except Exception, e:
            errormsg = "error receiving data " + str(e)
            buffer = ""
            print(errormsg)

        return buffer

    def list(self, vb_id, extra=None):

        status = self.connect()
        if status == False:
            return -1

        if extra != None:
            send_cmd = "LIST " + str(vb_id) + " " + extra
        else:
            send_cmd = "LIST " + str(vb_id)

        try:
            self.sock.sendall(send_cmd)
        except Exception, e:
            print >> sys.stderr, "problem happen", str(e)

        buffer = self.read_data()
        self.sock.close()
        if "Error" in buffer:
            return -1, None
        elif "Success" in buffer:
            return 0, None
        else:
            return 0, buffer

    def remove(self, vb_id, extra):

        status = self.connect()
        if status == False:
            return -1

        send_cmd = "REMOVE " + str(vb_id) + " " + extra

        try:
            self.sock.sendall(send_cmd)
        except Exception, e:
            print >> sys.stderr, "problem happen", str(e)

        buffer = self.read_data()
        self.sock.close()
        if "Success" in buffer:
            return True
        else:
            return False

    def add_lock(self, vb_id, extra):

        status = self.connect()
        if status == False:
            return -1

        send_cmd = "ADDLOCK " + str(vb_id) + " " + extra

        try:
            self.sock.sendall(send_cmd)
        except Exception, e:
            print >> sys.stderr, "problem happen", str(e)

        buffer = self.read_data()
        self.sock.close()
        if "Success" in buffer:
            return True
        else:
            return False

    def download(self, vb_id, filename, output_file=None):

        status = self.connect()
        if status == False:
            return -1, None

        send_cmd = "DOWNLOAD " + str(vb_id) + " " + filename

        try:
            self.sock.sendall(send_cmd)
        except Exception, e:
            print >> sys.stderr, "problem happen", str(e)

        buffer = self.read_data()
        self.sock.close()

        if len(buffer) > 0 and output_file != None:
            try:
                file = open(output_file, "w+")
                file.write(buffer)
                file.close()
            except Exception, e:
                print ("failed to write", str(e))
        else:
            return -1, None


        #if the output file is specified then dont return the buffer
        if output_file != None and os.path.getsize(output_file) > 0:
            return 0, "download successful"
        else:
            return 0, buffer


if __name__ == '__main__':

    download_instance = download_client("172.21.13.73", 22122)
    buffer = download_instance.list(1, "incremental")
    print len(buffer)
    buffer = download_instance.list(1, "woohah")
    print len(buffer)
    status, buffer = download_instance.download(1, "incremental/vb_1_backup-2013-04-04_13:43:10-00000.mbb", "myfile.txt")
    if status == 0:
        print len(buffer)
    status = download_instance.add_lock(1, "incremental/somefile.lock")
    print status
    status = download_instance.remove(1, "incremental/somefile.lock")
    print status
