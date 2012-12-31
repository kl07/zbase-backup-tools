#!/usr/bin/env python

import commands
import os
import sys

def list_files(dirname):
    status, output = commands.getstatusoutput('find %s -type f \( -name "*.mbb" -o -name "*.split" -o -name "manifest.del" -o -name "done" -o -name "complete" -o -name "done-*" -o -name "merged-*" \)' %dirname)
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
    new, removed = list(set(l2) - set(l1)), list(set(l1) - set(l2))
    newlist = []
    for ftype in [".mbb", ".split", "manifest.del", "done-", "merged-", "done", "complete"]:
        for line in new[:]:
            if ftype in line:
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
