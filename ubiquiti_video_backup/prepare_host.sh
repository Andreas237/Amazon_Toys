#!/bin/bash


setup_aws_cli(){
    # Install tools needed for AWS
    npm install -g aws-cdk@2.51.1
    python3 -m venv .venv
    source .venv/bin/activate
    pip3 install -r requirements.txt

    export ACCOUNT_ID=$(aws sts get-caller-identity --query Account | tr -d '"')
    if test -z ${ACCOUNT_ID}
        printf "Unable to determine AWS Account ID.  Have you run >>aws configure<< ?"
        exit 1
    fi

    export AWS_REGION=$(aws configure get region)
    if test -z ${AWS_REGION}
        printf "Unable to determine AWS Account Region.  Have you run >>aws configure<< ?"
        exit 1
    fi

    cdk bootstrap aws://${ACCOUNT_ID}/${AWS_REGION}

    printf "[DEBUG]\tfinished setup_aws_cli()\n"
}


#TODO: add a cronjob to run main.py

setup_aws_cli
