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

RUST_VERSION="$(cat ./rust-toolchain | tr -d '[:space:]')"
echo "rust version: $RUST_VERSION"

echo "building $PACKAGE..."
docker build --build-arg RUST_VERSION=$RUST_VERSION -t $PACKAGE -f docker/Dockerfile --force-rm .
