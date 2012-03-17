#!/bin/bash
#Description: backup verfication script

inputdir=$1
outputdir=$2

[ $# -ne 2 ] && echo "Usage: $0 inputdir outputdir" && exit 1

if [ ! -d $1 ] || [ ! -d $2 ];
then
    echo "Usage: $0 inputdir outputdir"
    exit 1
fi

for i in $inputdir/*.mbb;
do
    echo $i
    echo ".dump" | sqlite3 $i | grep 'INSERT INTO "cpoint_op"' >> tmp
done

sort tmp -o input.tmp
rm tmp


for i in $outputdir/*.mbb;
do
    echo $i
    echo ".dump" | sqlite3 $i | grep 'INSERT INTO "cpoint_op"' >> tmp
done

sort tmp -o output.tmp
rm tmp

diff input.tmp output.tmp
rm input.tmp output.tmp
