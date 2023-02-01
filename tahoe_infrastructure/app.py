#!/usr/bin/env python3
import os
import subprocess
import json
from aws_cdk import *
import hashlib
import boto3
import botocore
import logging

from requests import JSONDecodeError
from stacks.base.tahoe_infrastructure_stack import TahoeInfrastructureStack
from stacks.base.tahoe_sandbox_stack import TahoeSandboxStack
from stacks.base.tahoe_singleton_stack import TahoeSingletonStack, TahoeSingletonStack
from stacks.shared.lib.build_config import BuildConfig
from stacks.shared.lib.regional_config import SandboxConfig

import logging.config


app = App()


def create_packages():
    os.chdir("../base/common")
    cache = None
    comp_cache = {}
    try:
        os.makedirs("cache")
    except FileExistsError:
        logger.info("cache active!")
    except Exception as e:
        raise e
    try:
        cache = json.loads(open(os.path.join(os.getcwd(), "cache", "cache.json"), "r").read())
    except JSONDecodeError:
        raise JSONDecodeError("Could not parse cache file try deploying again")
    except IOError as e:
        logger.info("Creating cache file since one does not exist")
    except Exception as e:
        logger.info(str(e))

    comp_cache["tahoelogger"] = create_hash_from_path(os.path.join(os.getcwd(), "tahoelogger")).hexdigest()
    comp_cache["tahoecommon"] = create_hash_from_path(os.path.join(os.getcwd(), "tahoecommon")).hexdigest()

    logger.debug(f"New hashes: {comp_cache}")
    logger.debug(f"Current hashes {cache}")
    print(cache)
    if cache is None or (
        comp_cache["tahoelogger"] != cache["tahoelogger"] or comp_cache["tahoecommon"] != cache["tahoecommon"]) or len(
        os.listdir(os.path.join(os.getcwd(),
                                "dist"))) < 2:
        build_whls(comp_cache)
    os.chdir("../")


def build_whls(new_cache):
    logger.info("BUILDING")
    with open(os.path.join(os.getcwd(), "cache", "cache.json"), "w+") as file:
        os.chdir("../../bin")
        p = subprocess.Popen(["./build_common_whls.sh"], shell=True, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if out and not err:
            logger.info("build_common_whls.sh has run")
        else:
            logger.critical("build_common_whls.sh script failure")
            raise ValueError("build_common_whls.sh script failed to run")
        cache = new_cache
        json.dump(cache, file)
        os.chdir("../base/common")


def create_hash_from_path(path, hash=None):
    if hash is None:
        hash = hashlib.md5()

    for files in os.listdir(path):
        if files != "__pycache__":
            if os.path.isdir(os.path.join(path, files)):
                hash = create_hash_from_path(os.path.join(path, files), hash)
            else:
                with open(os.path.join(path, files), "r") as f:
                    while True:
                        chunk = f.read(65536).encode()
                        if not chunk:
                            break
                        hash.update(chunk)
    return hash


def get_parameter(env,  name):
    if name in env:
        return env[name]
    else:
        raise ValueError(
            "Missing parameter '{}' from build environment in cdk.json".format(name))


def getConfiguration():
    env = app.node.try_get_context('env')
    if not env:
        if "USER" in os.environ:
            env = os.environ['USER']
        else:
            raise NameError("No valid environment found")
    build_env_file = app.node.try_get_context(env)
    if build_env_file is None:
        raise NameError(f"{env} is not in in environment")
    with open(f"{build_env_file}{os.sep}{env}.json") as file:
        build_env = json.load(file)
        external_resources = app.node.try_get_context("external_resources")
        bc = BuildConfig(get_parameter(build_env, "stack_prefix"),
                         get_parameter(build_env, "s3_prefix"),
                         os.environ["CDK_DEFAULT_REGION"],
                         env,
                         os.environ["CDK_DEFAULT_ACCOUNT"],
                         get_parameter(build_env, "sandbox"),
                         external_resources,
                         datasource_config=get_parameter(build_env, "datasources"),
                         tags=get_parameter(build_env, "tags"),
                         redshift_schemas=get_parameter(build_env, "redshift_schemas"),
                         cdk_log_level=get_parameter(build_env, "cdk_log_level"),
                         credentials=get_parameter(build_env, "credentials"),
                         optional=get_parameter(build_env, "optional"))

    return bc


def init_logger(config: BuildConfig):
    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': config.get_cdk_log_level(),
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': config.get_cdk_log_level(),
                'propagate': False
            }
        }
    }
    logging.config.dictConfig(LOGGING_CONFIG)


def bootstrap(config: BuildConfig):
    cf = boto3.client('cloudformation')
    try:
        cf.describe_stacks(StackName="CDKToolkit")
    except botocore.exceptions.ClientError:
        subprocess.Popen(
            "cdk bootstrap --profile {}".format(config.get_profile()), shell=True)


if __name__ == "__main__":

    config = getConfiguration()
    init_logger(config)
    logger = logging.getLogger(__name__)
    create_packages()

    logger.info(f"REGION {os.environ['CDK_DEFAULT_REGION']}")
    logger.info(f"ACCOUNT {os.environ['CDK_DEFAULT_ACCOUNT']}")

    synths = []
    # Each stack requires its own synth object
    for s in range(3):
        if "gov" in os.environ["CDK_DEFAULT_REGION"]:
            # uses gov cloud llnl role names
            synth = DefaultStackSynthesizer(
                bucket_prefix=config.get_s3_prefix() + "/",
                cloud_formation_execution_role="arn:aws-us-gov:iam::${AWS::AccountId}:role/LLNL_User_Roles_cdk-hnb659fds-exec-${AWS::AccountId}-${AWS::Region}",
                deploy_role_arn="arn:aws-us-gov:iam::${AWS::AccountId}:role/LLNL_User_Roles_cdk-hnb659fds-deploy-${AWS::AccountId}-${AWS::Region}",
                image_asset_publishing_role_arn="arn:aws-us-gov:iam::${AWS::AccountId}:role/LLNL_User_Roles_cdk-hnb659fds-image-${AWS::AccountId}-${AWS::Region}",
                file_asset_publishing_role_arn="arn:aws-us-gov:iam::${AWS::AccountId}:role/LLNL_User_Roles_cdk-hnb659fds-file-${AWS::AccountId}-${AWS::Region}",
                lookup_role_arn="arn:aws-us-gov:iam::${AWS::AccountId}:role/LLNL_User_Roles_cdk-hnb659fds-lookup-${AWS::AccountId}-${AWS::Region}")
        else:
            # uses default role names for public deployment
            synth = DefaultStackSynthesizer(
                bucket_prefix=config.get_s3_prefix() + "/")
        synths.append(synth)
    logger.info(f"SANDBOX STATUS: {config.is_sandbox()}")
    if config.is_sandbox():
        # Creates servers for sandbox regions/account
        sandboxStack = TahoeSandboxStack(app,
                                         "TahoeSandboxStack",
                                         build_context=config,
                                         synthesizer=synths[0],
                                         env=Environment(
                                             region=os.environ["CDK_DEFAULT_REGION"],
                                             account=os.environ["CDK_DEFAULT_ACCOUNT"]))
        # Override provided value if sandbox is created
        config.set_credential("tahoe-redshift-secret-name", sandboxStack.redshift.secret.secret_name)

    # Stack that generated lakeformation setup
    singleStack = TahoeSingletonStack(app,
                                      "TahoeInitialStack",
                                      build_context=config,
                                      synthesizer=synths[1],
                                      env=Environment(
                                          region=os.environ["CDK_DEFAULT_REGION"],
                                          account=os.environ["CDK_DEFAULT_ACCOUNT"]))
    # Stack that handles creating location specific stacks
    baseStack = TahoeInfrastructureStack(app,
                                            config.get_stack_prefix(),
                                            build_context=config,
                                            synthesizer=synths[2],
                                            env=Environment(
                                                region=os.environ["CDK_DEFAULT_REGION"],
                                                account=os.environ["CDK_DEFAULT_ACCOUNT"]))
    app.synth()
