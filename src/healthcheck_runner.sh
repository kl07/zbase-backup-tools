#!/bin/bash
#Description: Run health check and send mail if any criticals

game_id=`grep 'game_id =' /etc/membase-backup/default.ini | awk '{ print $NF }'`
cloud_id=`grep 'cloud =' /etc/membase-backup/default.ini | awk '{ print $NF }'`
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
/opt/membase/membase-backup/backup_healthcheck -g $game_id -c $cloud_id -o $rep_dir $count &> /tmp/rep-$$

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



