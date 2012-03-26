#!/bin/bash
#Description: Count the uniq keys from the backup list

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


