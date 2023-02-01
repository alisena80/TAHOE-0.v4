# CDK commands
* python 3.6<=
* nodejs 13<=
* npm install aws-cdk
* cdk init app --language python



## Local CDK installation guide

Prerequisites: AWS CLI and secret keys

1. All AWS CDK developers, even those working in Python, Java, or C#, need Node.js 10.13.0 or later. (Node 13 not compatible) 
    * https://nodejs.org/en/

2. aws configure credentials and region of deployment location. (If you use a named profile from the credentials file, the config must have a profile of the same name specifying the region.)

3. Python 3.6 or later including pip and virtualenv 
    * https://www.python.org/downloads/
    * https://pypi.org/project/virtualenv/

4. sudo npm install -g aws-cdk

5. In root tahoe infrastructure folder
    * python -m venv .venv

6. Activate venv and install dependencies
    * source .venv/bin/activate
    * python3 -m pip install -r requirements.txt
    * if using vscode add ./tahoe_infrastructure/.venv/bin/python to be the python interpreter path
7. Set environment
    * Create new environment in cdk.json
    * pass into cdk commands as: '''--context env=garg4''' or Add "USERNAME" in os environment variables or



## Docker set up
1. Location of Dockerfile: `docker build -t tahoe-cdk .` if in Livermore: run `docker build --build-arg SSL=ssl -t tahoe-cdk .` to add cert vpn requirements
2. `docker container run --name cdk-builder -it tahoe-cdk /bin/bash`
3. Get code base on docker: `docker cp tahoe.zip cdk-builder:cdk-launch` in host machine or `cd cdk-launch && git clone` in docker container
4. In docker container:
`aws configure` 
`cd cdk-launch`
`unzip tahoe.zip`
`cd tahoe`
`source docker_setup.sh`
`cd tahoe_infrastructure`
`source .venv/bin/activate`

Additional commands
Start docker if stopped: `docker start cdk-builder` 
To create new shell when docker is running: `docker exec -it cdk-builder /bin/bash` 

1. Govcloud bootstrap
aws cloudformation create-stack --stack-name CDKToolkit --template-body file://bootstrap-template.yaml --parameters ParameterKey=PermissionsBoundaryPolicy,ParameterValue=default-modified-permissions-boundary --capabilities CAPABILITY_NAMED_IAM
Add --role-arn arn:aws-us-gov:iam::398592312202:role/LLNL_User_Roles_Bootstrap if using role with cdk_user permissions. Cdk user requires Pass Role/ Assume Role on prefix LLNL_User_Roles

2. Test valid stack
cdk diff --context env={cdk.json env}
cdk synth --context env={cdk.json env}
cdk deploy --context env={cdk.json env} --all

# Read only access
 Read access to console is given on a stack basis currently.

 To give a user read only access as well as basic manual step function/lambda invokation permissions:

 1. Add user to the user group titled LLNL_User_Roles_{stack_prefix}_ReadOnly. This user group allows users to assume the LLNL_User_Roles_{stack_prefix}_ReadOnly role to access console for tahoe resources.

 2. The user can switch into the role once logged into thier user using a url like: 
 https://signin.aws.amazon.com/switchrole?roleName=LLNL_User_Roles_{stack_prefix}_ReadOnly&account=123456789012
 
 Or

 Use the switch role button in the console and input the details visually.


build tahoecommons 
whl python setup.py bdist_wheel --universal

## Line ending errors
sed -i 's/\r//' docker_setup.sh
sed -i 's/\r//' build_common_whls.sh



# AWS best practices 
https://aws.amazon.com/blogs/devops/best-practices-for-developing-cloud-applications-with-aws-cdk/

### Reasoning for singular cdk bootstrap per region account combination
The qualifier is added to the name of bootstrap resources to distinguish the resources in separate bootstrap stacks. To deploy two different versions of the bootstrap stack in the same environment (AWS account and region), then, the stacks must have different qualifiers. This feature is intended for name isolation between automated tests of the CDK itself. Unless you can very precisely scope down the IAM permissions given to the AWS CloudFormation execution role, there are no privilege isolation benefits to having two different bootstrap stacks in a single account, so there is usually no need to change this value.

### Best practices not supported due to compliance issues/iam management
1. A better approach is to specify as few names as possible. If you leave out resource names, the AWS CDK will generate them for you, and it'll do so in a way that won't cause these problems. You then, for example, pass the generated table name (which you can reference as table.tableName in your AWS CDK application) as an environment variable into your AWS Lambda function, or you generate a configuration file on your Amazon EC2 instance on startup, or you write the actual table name to AWS Systems Manager Parameter Store and your application reads it from there. 

2. One of the great features of the AWS CDK construct library is the convenience methods that have been built in to the resources to allow quick and simple creation of AWS Identity and Access Management (IAM) roles. We have designed that functionality to allow minimally-scoped permissions for the components of your application to interact with each other. For example, consider a typical line of code like the following:
# Datasources
Consists of
 1. Download Step workflow
 2. Interim Etl step workflow
 3. Glue data databases: interim/raw curated(maybe shifted to consumer table)
 4. Sns topics after download step and after interim step

# Consumers
1 datasource can trigger multiple consumers and 1 consumer can be triggered by multiple datasources if the resource is combine different sources of data (Common use case).
If multiple datasources are not being combined is interim == curated.  
 1. Consumer specific curated workflow (Etl/ Databrew)
 2. Consumer specific location configurations ie redshift/elasticsearch/quicksight

# API

API's to access curated data

# API Documentation
The API documentation is generated in the lambda function. It uses Python classes that are hooked into a library called APISpec and another library called Marshmallow. These two auto-inspect the Python classes to
generate definitions in the OpenAPI spec. Then the documentation lives along side each endpoint for the bodies and return values. Then, to access the `swagger.json`, a user needs to request the `/openapi` endpoint. That endpoint returns the OpenAPI spec for the documentation, which can be placed in any viewer of choice (i.e. [editor.swagger.io](editor.swagger.io)). A user may use the viewer to explore and make requests to the API, but you must first add the `X-API-Key` in the UI for authentication. The base URL is listed at the top of the editor UI, and should reference the same lambda base URL that was used to request `/openapi`. 

The addition of other Python files necessitates a small change to the tahoe_infrastructure project. Now, on deploy to AWS, we copy every `*.py` file in the `base/api` project into the deployed zip. This allows using multiple python files in your lambda. 


# Databrew pipeline resource
https://aws.amazon.com/tw/blogs/big-data/set-up-ci-cd-pipelines-for-aws-glue-databrew-using-aws-developer-tools/
Databrew deployments using cdk are cumbersome expecially if recipes are already defined (cost to translate to python constructs)

# Redshift vs Redshift Spectrum vs Athena

Redshift:

Copies data from glue to redshift instance. Increases data cost storage due to copy and 
