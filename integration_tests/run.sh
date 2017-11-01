#!/bin/bash

LOCAL_PORT=8080
IMAGE_NAME=presto/test_server

cd "$( dirname "${BASH_SOURCE[0]}" )"

function test_container() {
	echo `docker ps | grep $IMAGE_NAME | cut -d\  -f1`
}

function test_cleanup() {
	docker rm -f `test_container`
	#docker rmi $IMAGE_NAME
}

trap test_cleanup EXIT

function test_build() {
	local image=`docker images | grep $IMAGE_NAME`
	[ -z "$image" ] && docker build -t $IMAGE_NAME .
}

function test_query() {
	docker exec -t -i `test_container` bin/presto-cli --server localhost:${LOCAL_PORT} --execute "$*"
}

test_build
docker run -p ${LOCAL_PORT}:${LOCAL_PORT} --rm -d $IMAGE_NAME

attempts=10
while [ $attempts -gt 0 ]
do
	attempts=`expr $attempts - 1`
	ready=`test_query "SHOW SESSION" | grep task_writer_count`
	[ ! -z "$ready" ] && break
	echo "waiting for presto..."
	sleep 2
done

if [ $attempts -eq 0 ]
then
	echo "timed out waiting for presto"
	exit 1
fi

PKG=../presto
DSN=http://test@localhost:${LOCAL_PORT}
go test -v -cover -coverprofile=coverage.out $PKG -presto_server_dsn=$DSN $*
