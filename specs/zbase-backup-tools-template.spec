Summary: ZBase backup and restore tools
Name: zbase-backup-tools
Version: _VERSION
Release: _RELEASE
Group: General
License: Proprietary
Source0: _SOURCE
Packager: Sarath Lakshman <slakshman@zynga.com>
AutoReqProv: no
Requires: jemalloc

%description
zbase backup and restore tools

%prep
%define _rpmfilename %%{NAME}-_COMMIT.%%{ARCH}.rpm
%setup

%install
mkdir -p $RPM_BUILD_ROOT/opt/zbase/zbase-backup/blobrestore_utils/
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
mkdir -p $RPM_BUILD_ROOT/etc/zbase-backup/

cp src/backup-merge/merge-incremental $RPM_BUILD_ROOT/opt/zbase/zbase-backup/
cp src/backuplib.py \
src/config.py \
src/consts.py \
src/daily-merge \
src/logger.py \
src/master-merge \
src/mc_bin_client.py \
src/zbase-backupd \
src/zbase-restore \
src/memcacheConstants.py \
src/util.py \
src/diffdisk.py \
src/backup_healthcheck \
src/count_backup_keys.sh \
src/healthcheck_runner.sh \
src/scheduler.py \
src/backup_merged \
src/zstore_cmd \
src/zstore_cmd_helper $RPM_BUILD_ROOT/opt/zbase/zbase-backup/
cp src/blobrestore_utils/* $RPM_BUILD_ROOT/opt/zbase/zbase-backup/blobrestore_utils/
cp conf/clean_blobrestore_jobs.cron $RPM_BUILD_ROOT/opt/zbase/zbase-backup/
cp conf/init.d/zbase-backupd $RPM_BUILD_ROOT/etc/init.d/
cp conf/init.d/blobrestored $RPM_BUILD_ROOT/etc/init.d/
cp conf/init.d/backup_merged $RPM_BUILD_ROOT/etc/init.d/
cp conf/default.ini $RPM_BUILD_ROOT/etc/zbase-backup/

%files
/etc/zbase-backup/*
/etc/init.d/zbase-backupd
/etc/init.d/blobrestored
/etc/init.d/backup_merged
/opt/zbase/zbase-backup/*

%post
ln -f -s /opt/zbase/zbase-backup/zstore_cmd   /usr/bin/zstore_cmd
ln -f -s /usr/bin/python2.6 /usr/bin/python26
