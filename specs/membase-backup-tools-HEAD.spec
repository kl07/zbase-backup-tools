Summary: Membase 1.7.3 backup and restore tools
Name: membase-backup-tools
Version: 1.2
Release: 2
Group: General
License: Proprietary
Source0: _SOURCE
Packager: Sarath Lakshman <slakshman@zynga.com>
AutoReqProv: no
Requires: jemalloc

%description
Membase 1.7.3 backup and restore tools

%prep
%define _rpmfilename %%{NAME}-_COMMIT.%%{ARCH}.rpm
%setup

%install
mkdir -p $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
mkdir -p $RPM_BUILD_ROOT/etc/membase-backup/

cp src/backup-merge/merge-incremental $RPM_BUILD_ROOT/opt/membase/membase-backup/
cp src/backuplib.py \
src/config.py \
src/consts.py \
src/daily-merge \
src/logger.py \
src/master-merge \
src/mc_bin_client.py \
src/membase-backupd \
src/membase-restore \
src/memcacheConstants.py \
src/util.py \
src/diffdisk.py \
src/backup_healthcheck \
src/count_backup_keys.sh \
src/healthcheck_runner.sh \
src/synctos3.sh \
src/scheduler.py \
src/backup_merged \
src/zstore_cmd \
src/zstore_cmd_helper $RPM_BUILD_ROOT/opt/membase/membase-backup/
cp src/blobrestore_utils/* $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/
cp conf/clean_blobrestore_jobs.cron $RPM_BUILD_ROOT/opt/membase/membase-backup/
cp conf/init.d/membase-backupd $RPM_BUILD_ROOT/etc/init.d/
cp conf/init.d/blobrestored $RPM_BUILD_ROOT/etc/init.d/
cp conf/init.d/backup_merged $RPM_BUILD_ROOT/etc/init.d/
cp conf/default.ini $RPM_BUILD_ROOT/etc/membase-backup/
chown root $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/blobrestore_sshkey
chmod 700 $RPM_BUILD_ROOT/opt/membase/membase-backup/blobrestore_utils/blobrestore_sshkey

%files
/etc/membase-backup/*
/etc/init.d/membase-backupd
/etc/init.d/blobrestored
/etc/init.d/backup_merged
/opt/membase/membase-backup/*

%post
ln -f -s /opt/membase/membase-backup/zstore_cmd   /usr/bin/zstore_cmd
ln -f -s /usr/bin/python2.6 /usr/bin/python26
