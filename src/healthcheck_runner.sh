#!/bin/bash
#Description: Run health check and send mail if any criticals

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

game_id=`grep 'game_id =' /etc/zbase-backup/default.ini | awk '{ print $NF }'`
rep_dir='/var/www/html/health_reports/'

count=""

if [ $# -ne 1 -a $# -ne 2 ];
then
    echo "Usage: $0 [ count ] emails=emailid1,emailid2.."
    exit 1
fi

if [[ "$1" = "count" ]];
then
    count=" -k"
fi

if [[ $1 =~ "emails" ]];
then
    emails="$(echo $1 | cut -d= -f2 | tr ',' ' ')"
fi

if [[ $2 =~ "emails" ]];
then
    emails="$(echo $2 | cut -d= -f2 | tr ',' ' ')"
fi

if [[ -z "$emails" ]];
then
    echo Email list empty
    exit 1
fi

mkdir -p $rep_dir
/opt/zbase/zbase-backup/backup_healthcheck -g $game_id -o $rep_dir $count &> /tmp/rep-$$

grep -q CRITICAL /tmp/rep-$$;
if [ $? -eq 0 ];
then
    echo Found criticals. Sending email
    for em in $emails;
    do
        cat /tmp/rep-$$ | mail -s "Backup Server: $HOSTNAME health report" $em
    done

fi
rm /tmp/rep-$$



