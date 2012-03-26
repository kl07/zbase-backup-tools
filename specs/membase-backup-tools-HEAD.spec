#Membase backup tools 1.0 - commit ID: e0478c0e8ea184c0cdf35074005967f7430f0f1b

Summary: Membase 1.7.3 backup and restore tools 
Name: membase-backup-tools
Version: 1.0
Release: 5
Group: General
License: Proprietary
Source0: _SOURCE 
Packager: Sarath Lakshman <slakshman@zynga.com>
AutoReqProv: no

%description 
Membase 1.7.3 backup and restore tools

%prep 
%define _rpmfilename %%{NAME}-_COMMIT.%%{ARCH}.rpm
%setup

%install 
mkdir -p $RPM_BUILD_ROOT/opt/membase/membase-backup/t/
mkdir -p $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
mkdir -p $RPM_BUILD_ROOT/etc/membase-backup/
mkdir -p $RPM_BUILD_ROOT/opt/membase/membase-backup/misc/

cp -r misc/mbbackup-merge-incremental $RPM_BUILD_ROOT/opt/membase/membase-backup/misc/
cp -r src/* $RPM_BUILD_ROOT/opt/membase/membase-backup/
cp -r conf/clean_blobrestore_jobs.cron $RPM_BUILD_ROOT/opt/membase/membase-backup/
cp -r src/t/* $RPM_BUILD_ROOT/opt/membase/membase-backup/t/
cp conf/init.d/membase-backupd $RPM_BUILD_ROOT/etc/init.d/
cp conf/init.d/blobrestored $RPM_BUILD_ROOT/etc/init.d/
cp conf/default.ini $RPM_BUILD_ROOT/etc/membase-backup/
chown root $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/blobrestore_sshkey
chmod 700 $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/blobrestore_sshkey

%files
/etc/membase-backup/*
/etc/init.d/membase-backupd
/etc/init.d/blobrestored
/opt/membase/membase-backup/*

%post
ln -f -s /opt/membase/membase-backup/zstore_cmd   /usr/bin/zstore_cmd
ln -f -s /usr/bin/python2.6 /usr/bin/python26
ln -f /opt/membase/membase-backup/misc/mbbackup-merge-incremental /opt/membase/lib/python/mbbackup-merge-incremental
