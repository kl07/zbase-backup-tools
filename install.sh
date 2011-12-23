#!/bin/bash
#Description: Incremental backup installer
PID=$$
installer_path=`dirname $(cat /proc/$PID/cmdline | tr '\0' '\n' | tail -1)`

if [ $UID -ne 0 ];
then
    echo Run as root
    exit 1
fi

echo Installing Incremental backup scripts
mkdir -p /etc/membase-backup && cp $installer_path/conf/default.ini /etc/membase-backup  && cp -r $installer_path/src/ /opt/membase/membase-backup/ && cp $installer_path/conf/init.d/mbbackup /etc/init.d/

if [ $? -eq 0 ];
then
    echo Installation completed successfully
    exit 0
else
    echo Installation failed
    exit 1
fi




