#!/bin/bash
#Description: RPM Build script

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

if [ $1 = "release" ];
then
    specfile=specs/zbase-backup-tools-HEAD.spec
    VERSION=`grep Version $specfile | awk '{ print $NF }'`
    RELEASE=`grep Release $specfile | awk '{ print $NF }'`
    COMMIT="$VERSION-$RELEASE"
else
    specfile=specs/zbase-backup-tools-template.spec
    VERSION=`git tag | head -1`
    RELEASE=`git log --oneline $VERSION.. | wc -l`
    COMMIT=`git describe`
fi

BUILD_TMP="$(pwd)/build"
rm -rf $BUILD_TMP
mkdir -p $BUILD_TMP/{SRPMS,RPMS,BUILD,SOURCE,BUILDROOT}
RPM_SOURCE_DIR=`rpm --define "_topdir $BUILD_TMP" --eval '%{_sourcedir}'`
SOURCE="zbase-backup-tools-$VERSION.tar.gz"
mkdir -p $RPM_SOURCE_DIR
cp $specfile tmp-$$.spec
sed -i "s/_VERSION/$VERSION/" tmp-$$.spec
sed -i "s/_SOURCE/$SOURCE/" tmp-$$.spec
sed -i "s/_RELEASE/$RELEASE/" tmp-$$.spec
sed -i "s/_COMMIT/$VERSION-$RELEASE/" tmp-$$.spec
git archive --format=tar --prefix="zbase-backup-tools-$VERSION/" HEAD | gzip > $RPM_SOURCE_DIR/$SOURCE
buildtmp=/tmp/build
mkdir $buildtmp
rpmbuild --define="_topdir $BUILD_TMP" --buildroot $BUILD_TMP/BUILDROOT -ba tmp-$$.spec
rm tmp-$$.spec
