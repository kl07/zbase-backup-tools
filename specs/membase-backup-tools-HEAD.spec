Summary: Membase 1.7.3 backup and restore tools 
Name: membase-backup-tools
Version: 1.0
Release: 1
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
/opt/membase/membase-backup/*

%post
ln -f -s /opt/membase/membase-backup/zstore_cmd   /usr/bin/zstore_cmd
ln -f -s /usr/bin/python2.6 /usr/bin/python26