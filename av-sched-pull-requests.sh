#!/bin/sh
set -e

export PATH=$PATH:/home/jenkins/npm/bin

# Global install of gulp
npm install -g gulp --quiet
npm install --quiet

# Build application itself
mvn package
