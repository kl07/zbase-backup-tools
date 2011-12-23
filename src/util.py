#!/usr/bin/env python26

import re
import os
tokenize = re.compile(r'(\d+)|(\D+)').findall
def natural_sortkey(string):
    return tuple(int(num) if num else alpha for num, alpha in tokenize(string))

def setup_sqlite_lib():
    ld_path = '/opt/sqlite3/lib/'
    if os.environ.has_key('LD_LIBRARY_PATH'):
        os.environ['LD_LIBRARY_PATH'] = "%s:%s" %(ld_path, os.environ['LD_LIBRARY_PATH'])
    else:
        os.environ['LD_LIBRARY_PATH'] = ld_path

