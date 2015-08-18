#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f $0))
export PATH=$PATH:/home/jenkins/npm/bin

function build {

  echo "Maven build av-sched"
 
  cd $SCRIPT_DIR/../..
  npm install --quiet
  mvn clean package -q -DskipTests

  echo "Docker build airvantage/av-sched"

  cd $SCRIPT_DIR
  rm -f *.jar
  cp ../../target/*-exec.jar ./av-sched.jar
  docker build -t airvantage/av-sched .

}

function deploy {

  echo "Docker push airvantage/av-sched"

  docker push airvantage/av-sched

}

while [ "$1" != "" ]; do
  case $1 in

    deploy )
      deploy
      ;;

    build )
      build
      ;;

    * )
      echo 'Usage : docker.sh build | deploy'
      exit 1

  esac
  shift

done

exit 0
