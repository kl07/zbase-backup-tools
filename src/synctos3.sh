#!/bin/bash
#Description: Sync server backups to s3

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

s3bucket='s3://membase-backup/storage_server_backup'
server_root='/var/www/html/membase_backup'
game_id=`grep 'game_id =' /etc/membase-backup/default.ini | awk '{ print $NF }'`
cloud_id=`grep 'cloud =' /etc/membase-backup/default.ini | awk '{ print $NF }'`

for host in `ls $server_root/$game_id`;
do
    date=`date "+%Y-%m-%d"`
    daily_dir="$server_root/$game_id/$host/$cloud_id/daily/$date"
    master_dir="$server_root/$game_id/$host/$cloud_id/master/$date"

#    if [ -d $daily_dir ];
#    then
#        echo "Found daily $daily_dir. Syncing to s3"
#        s3cmd sync $daily_dir/ $s3bucket/$HOSTNAME/$game_id/$host/$cloud_id/daily/$date/
#        [ $? -ne 0 ] && echo "Failed to upload $daily_dir"
#    fi

    if [ -d $master_dir ];
    then
        echo "Found master $master_dir. Syncing to s3"
        s3cmd sync $master_dir/ $s3bucket/$HOSTNAME/$game_id/$host/$cloud_id/master/$date/
        [ $? -ne 0 ] && echo "Failed to upload $master_dir"
    fi

done
