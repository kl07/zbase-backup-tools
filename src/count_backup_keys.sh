#!/bin/bash
#Description: Count the uniq keys from the backup list

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

pid=$$
trap "quit" SIGABRT SIGINT SIGQUIT SIGTERM SIGPIPE SIGSTOP SIGTSTP

quit () {
    ps -o pid= --ppid $pid | xargs kill -9
}

if [ $# -eq 0 ];
then
    echo "Usage: $0 backup1 backup2 backup2 ..."
    echo
    exit 1
fi

for f in $@;
do
    if [ ! -f $f ];
    then
        echo File $f not found.
        exit 1
    fi
done

for f in $@;
do
    echo 'select key from cpoint_op;' | sqlite3 $f;
done | sort -u | wc -l


