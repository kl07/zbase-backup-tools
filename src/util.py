#!/usr/bin/env python26

import re
import os
import shlex

tokenize = re.compile(r'(\d+)|(\D+)').findall
def natural_sortkey(string):
    return tuple(int(num) if num else alpha for num, alpha in tokenize(string))

def setup_sqlite_lib():
    ld_path = '/opt/sqlite3/lib/'
    if os.environ.has_key('LD_LIBRARY_PATH'):
        os.environ['LD_LIBRARY_PATH'] = "%s:%s" %(ld_path, os.environ['LD_LIBRARY_PATH'])
    else:
        os.environ['LD_LIBRARY_PATH'] = ld_path

def getcommandoutput(cmd, queue):
    """Return (status, output) of executing cmd in a shell."""
    """Add the process object to the queue"""
    import subprocess
    args = shlex.split(cmd)
    p = subprocess.Popen(args, shell=False, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if queue != None:
        queue.put(p)

    output = str.join("", p.stdout.readlines())
    sts = p.wait()

    if sts is None:
        sts = 0
    return sts, output
