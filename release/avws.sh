#!/bin/bash

SCRIPT_DIR=`dirname $(realpath $0)`

# Name of the repo where to upload the resources (artifact, template, ...)
REPO_NAME=av-sched

# Configuration files located on AWS S3 buckets
# The name of the configuration to be used is defined by the argument '--aws-cfg <ConfigName>'
# AWS_S3_BUCKETS[ConfigName]="s3://...""
declare -A AWS_S3_BUCKETS
AWS_S3_BUCKETS[prodna]="s3://av-prod-secrets/files/config/av-sched-na.cfg"
AWS_S3_BUCKETS[prodeu]="s3://av-prod-secrets/files/config/av-sched-eu.cfg"
AWS_S3_BUCKETS[perf]="s3://av-preprod-secrets/files/config/av-sched-perf.cfg"
AWS_S3_BUCKETS[dev1]="s3://av-preprod-secrets/files/config/av-sched-dev1.cfg"
AWS_S3_BUCKETS[qa]="s3://av-preprod-secrets/files/config/av-sched-qa.cfg"

# List of the network parameters to extract
NETWORK_PARAMS_NAMES=( PrivateSubnetA PrivateSubnetB PrivateSubnetC PrivateSecurityGroup KeyPair EnvType S3LoggingBucket AlarmSNSTopic )

# List of stack parameters (used in the help message)
STACK_PARAMS_NAMES=( Version EnvName InstanceType InstanceNumber DBUser DBPwd AvSchedSecret HealthCheckGracePeriod )

# List of secured stack parameters (used to hide the value of the parameters)
SECURED_STACK_PARAMS_NAMES=( DBPwd AvSchedSecret )


# ----------------------------------------------------------------------------------------
#
# Build all required artifacts for your application and upload them on S3.
# This function is called when the command 'build' is used.
#
# ----------------------------------------------------------------------------------------

function build {

    print "Build Artifact" 6

    initial_dir=`pwd`

    cd $SCRIPT_DIR/..
    npm install
    mvn clean package -q

    # Back to the initial directory
    cd $initial_dir
}

# ----------------------------------------------------------------------------------------
#
# Upload the artifacts to S3.
# The variable 'VERSION' contains the version of the application to release.
# This function is called when the command 'release' is used.
#
# ----------------------------------------------------------------------------------------

function release {

    print "Upload resources to S3" 6

    print "Uploading artifact" 2
    upload_to_repo $SCRIPT_DIR/../target/av-sched-*-exec.jar av-sched.jar

    print "Uploading cloudformation" 2
    upload_template $SCRIPT_DIR/cloudformation/av-sched.template
}


# ----------------------------------------------------------------------------------------
#
# Initialize the required variables to be used to deploy the stack.
# The following variables need to be set: ENV_NAME NETWORK_STACK STACK_NAME.
# The variable 'VERSION' contains the version of the application to deploy.
# This function is called when the command 'deploy' is used.
#
# ----------------------------------------------------------------------------------------

function prepare_deploy {
    # Read the parameter EnvName
    ENV_NAME=$(get_parameter EnvName)
    NETWORK_STACK=$ENV_NAME-net
    STACK_NAME=$ENV_NAME-av-sched

    # Set the parameter 'Version' as a stack parameter
    set_parameter Version $VERSION
}


## ==================================================================================
##
##                                       MAIN
##
## ==================================================================================

aws s3 cp s3://av-repo/tools/avws-tools.sh $SCRIPT_DIR
source $SCRIPT_DIR/avws-tools.sh
