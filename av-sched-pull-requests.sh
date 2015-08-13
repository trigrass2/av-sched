#!/bin/sh
set -e

export PATH=$PATH:/home/jenkins/npm/bin

# Global install of gulp
npm install -g gulp --quiet
npm install -g grunt --quiet
npm install --quiet

# Build application itself
mvn package

# Start a mysql docker container

export AV_SCHED_CONF_DIR=$WORKSPACE/conf/jenkins

echo "Starting mysql"
(sudo docker run -d --name="mysql-sched" -p 3306:3306 -e MYSQL_SCHEMA_NAME="av_sched" -e MYSQL_ADMIN_LOGIN="jenkins" -e MYSQL_ADMIN_PASS="jenkins" airvantage/dev-mysql)

echo "Starting server"
(java -jar ./target/av-sched-0.0.1-exec.jar --clear) & sched_pid=$!

cd $WORKSPACE/src/node/av-sched-test
npm install --quiet
grunt

kill $sched_pid || true
