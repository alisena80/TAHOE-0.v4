# #!/usr/bin/env bash
build_wheels="false"
build_common="false"
help="false"
OPTIND=1
SCRIPTDIR=`dirname $0`
SCRIPTNAME=`basename $0`

func_log() {
    NOW=`date '+%Y%m%d-%H%M%S'`
    echo "$SCRIPTNAME : $NOW : $*"
}
func_error() {
    NOW=`date '+%Y%m%d-%H%M%S'`
    echo "$SCRIPTNAME : $NOW : ERROR : $*"
    echo "$SCRIPTNAME : $NOW : EXITING SCRIPT !!!"
    exit
}

help_print(){
    echo "-w | --build_wheels: Builds glue wheelhouse"
    echo "-c | --build_common: Builds tahoe packages required on code updates"
    echo "--help: Get help on cli commands"
}

while getopts w:c:-: flag
do
    if [ $flag = "-" ]
    then
        flag="${OPTARG%%=*}"
        # get arg by removing flag name and = sign
        OPTARG="${OPTARG#$flag}"
        OPTARG="${OPTARG#=}"
    fi
    echo $flag $OPTARG
    case "${flag}" in
        help) help="true";;
        w | build_wheels) build_wheels=${OPTARG};;
        c | build_common) build_common=${OPTARG}
    esac
done
if [ "$help" = "true" ]
then
    help_print
fi
if [ "$build_wheels" = "true" ]
then
    (cd ../base/wheelhouse && source wheel_house.sh)
fi 

if [ "$build_common" = "true" ]
then
    (cd ../base/common && python setup-common.py bdist_wheel --universal)
fi 


# # wrapper for bootstrap use sts or user
# auth='USER'
# profile='default'
# path=''




# if [ $bootstrap = "ASSUME" ] 
# then
# ACCOUNT_ID=($(aws sts get-caller-identity --profile "${profile}" --query 'Account'))
# if [ $path != '']
# then
#     role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${path}/bootstrap_deploy"
# fi

# else
#     role_arn="arn:aws:iam::${ACCOUNT_ID}:role/bootstrap_deploy"
# fi

# echo $ACCOUNT_ID
# echo $profile
# echo $bootstrap

# # role=($(aws sts assume-role --role-arn "${role_arn}" --role-session-name bootstrap_role --query '[Credentials.AccessKeyId,Credentials.SecretAccessKey,Credentials.SessionToken]' --output text))
# # unset AWS_SECURITY_TOKEN
# # export AWS_ACCESS_KEY_ID=${role[0]}
# # export AWS_SECRET_ACCESS_KEY=${role[1]}
# # export AWS_SESSION_TOKEN=${role[2]}
# # export AWS_SECURITY_TOKEN=${role[2]}

# aws cloudformation create-stack --stack-name CDKToolkit --template-body file://../tahoe_infrastructure/bootstrap-template.yaml --parameters ParameterKey=Path,ParameterValue=/llnl-user/ ParameterKey=PermissionsBoundaryPolicy,ParameterValue=arn:aws-us-gov:iam::398592312202:policy/llnl-inf-permissions-boundary --profile ${profile}

# fi
# #     

# #     export AWS_DEFAULT_REGION="us-east-2"

# #     KST=($(aws sts assume-role --role-arn "${role_arn}" --role-session-name bootstrap_role --query '[Credentials.AccessKeyId,Credentials.SecretAccessKey,Credentials.SessionToken]' --output text))
# #     unset AWS_SECURITY_TOKEN
# #     export AWS_ACCESS_KEY_ID=${KST[0]}
# #     export AWS_SECRET_ACCESS_KEY=${KST[1]}
# #     export AWS_SESSION_TOKEN=${KST[2]}
# #     export AWS_SECURITY_TOKEN=${KST[2]}

# #     # Now you have assumed the role and obtained temporary credentials.
# #     aws cloudformation create-stack --stack-name CDKToolkit --template-body file://bootstrap-template.yaml
# # else

# # cdk bootstrap --profile "${profile}"

# # fi
#!/bin/bash

# generates an .EGG package
# cd python/
# rm -rf build/ dist/
# python setup-common.py bdist_egg

# Glue Python jobs must be create with the following parameter
# --extra-py-files s3://$STACKNAME_code/python/common-0.1-py2.7.egg
