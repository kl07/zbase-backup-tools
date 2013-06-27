#!/bin/bash
#Description: backup verfication script

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
