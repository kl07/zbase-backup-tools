#!/bin/bash
#Description: RPM Build script

RPM_SOURCE_DIR=`rpm --eval '%{_sourcedir}'`
VERSION=`git tag | head -1`
RELEASE=`git log --oneline $VERSION.. | wc -l`
SOURCE="membase-backup-tools-$VERSION.tar.gz"
COMMIT=`git describe`
mkdir -p $RPM_SOURCE_DIR
cp membase-backup-tools.spec tmp-$$.spec
sed -i "s/_VERSION/$VERSION/" tmp/membase-backup-tools.spec
sed -i "s/_SOURCE/$SOURCE/" tmp/membase-backup-tools.spec
sed -i "s/_RELEASE/$RELEASE/" tmp/membase-backup-tools.spec
sed -i "s/_COMMIT/$COMMIT/" tmp/membase-backup-tools.spec
git archive --format=tar --prefix="membase-backup-tools-$VERSION/" HEAD | gzip > $RPM_SOURCE_DIR/$SOURCE
rpmbuild -ba tmp-$$.spec
