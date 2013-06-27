#!/usr/bin/env python

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

import os
import sys
from util import getcommandoutput

def list_files(dirname):
    status, output = getcommandoutput('find %s -type f \( -name "*.mbb" -o -name "*.split" -o -name "manifest.del" -o -name "done" -o -name "complete" -o -name "done-*" -o -name "merged-*" \)' %dirname)
    if status == 0:
        return [ x for x in output.split('\n') if x != '']
    else:
        return []

def read_list(filename):
    l = []
    if os.path.exists(filename):
        f = open(filename, "r")
        l = map(lambda x: x.strip(), f.readlines())
    return l

def write_list(filename, l):
    f = open(filename, 'w')
    for i in l:
        f.write("%s\n" %i)
    f.close()


def diff_list(l1, l2):
    # If a manifest.del appears, always add to list
    for f in l1[:]:
        if "manifest.del" in f:
            l1.remove(f)

    new, removed = list(set(l2) - set(l1)), list(set(l1) - set(l2))
    newlist = []
    for ftype in [".mbb", ".split", "merged-", "done", "complete", "done-", "manifest.del"]:
        for line in sorted(new[:]):
            if ftype in line:
                if ftype == "done" and not line.endswith("done"):
                    continue
                newlist.append(line)
                new.remove(line)

    return newlist, removed

def dirdiff(basedir, dirname, manifest=".diffdata"):
    dirty, deleted = set([]), set([])
    curr_files = list_files(os.path.join(basedir, dirname))
    recent_list = read_list(os.path.join(basedir, manifest))
    dirty, deleted = diff_list(recent_list, curr_files)
    write_list(os.path.join(basedir, manifest), curr_files)

    return dirty, deleted

if __name__ == '__main__':
    a, b = dirdiff(sys.argv[1], sys.argv[2])
    print "New files"
    for i in a:
        print i

    print
    print "Deleted files"
    for i in b:
        print i
