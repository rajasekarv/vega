#!/bin/bash
SCRIPT_PATH=`dirname $(readlink -f $0)`

VERSION=$1
if [ -z $VERSION ]
then
VERSION='latest'
fi
PACKAGE="native_spark:${VERSION}"

cd $SCRIPT_PATH && cd ..
echo "work dir: $(pwd)"
echo "building $PACKAGE..."
docker build -t $PACKAGE -f docker/Dockerfile --force-rm .
