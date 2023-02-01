# TAHOE 


# Configurations
1. Deployment Properties file
~/mytahoe.env
	export AWS_ACCESS_KEY_ID=<removed>
	export AWS_SECRET_ACCESS_KEY=<removed>
	export AWS_DEFAULT_REGION=us-west-1

	export CDK_DEFAULT_ACCOUNT=637997685644
	export CDK_DEFAULT_REGION=$AWS_DEFAULT_REGION

	DEPLOYMENT=tang41
			  
- At deployment time, CDK code should pick up info from configurations properties from deployment file or env variables :
	MyDevStack(app, "dev", env=core.Environment(
		account=os.environ.get("CDK_DEPLOY_ACCOUNT", os.environ["CDK_DEFAULT_ACCOUNT"]),
		region=os.environ.get("CDK_DEPLOY_REGION", os.environ["CDK_DEFAULT_REGION"])


- the unique stack name will be generated as such :
		STACKNAME = tahoe_ + AWS_DEFAULT_REGION + DEPLOYMENT 
				  = tahoe_w1_tang41

- Tagging - all resources must be tagged with below:
	project = tahoe
	deployment = $STACKNAME
	lastupdate = $DATE
	version = 1.01

# CloudFormation Stacks

## Share stack - system-wide per region single instance
	- Data lake formation
		Data Lake Admin User = LLNL_User_datalake_admin
	- DocumenDB
	- Redshift

## Base Stack - unique per stack
	BASTSTACKNAME = $STACKNAME_BASE

	- IAM
		LLNL_User_Policies_$STACKNAME_Base_Policy 
			- 1 policy for internal pipeline
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Sid": "AllowPublishSNSTopics",
            "Effect": "Allow",
            "Action": [
                "SNS:Publish",
                "SNS:Subscribe"
            ],
            "Resource": "arn:aws:sns:us-west-1:637997685644:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3-object-lambda:*"
            ],
            "Resource": [
                "arn:aws:s3:::tahoe-w1-tang41-data/*",
                "arn:aws:s3:::tahoe-w1-tang41-code/*",
                "arn:aws:s3:::tahoe-w1-tang41-tmp/*",
				"arn:aws:s3:::tahoe-w1-tang41-db/*"
            ]
        },
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "glue:*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "states:*"
            ],
            "Resource": [
                "arn:aws:states:us-west-1:637997685644:stateMachine:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "databrew:*"
            ],
            "Resource": [
                "arn:aws:databrew:us-west-1:637997685644:job/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:*"
            ],
            "Resource": "*"
        }
    ]
}

		LLNL_User_Role_$STACKNAME_Base_Role 
			- all internal services will run in this role
			- Trusted entities
				The identity provider(s) lambda.amazonaws.com
				The identity provider(s) states.amazonaws.com
				The identity provider(s) glue.amazonaws.com
				The identity provider(s) lakeformation.amazonaws.com
			- with policy :
				LLNL_User_Policies_$STACKNAME_Base_Policy 

		LLNL_User_Group_$STACKNAME
			- has users
				LLNL_User_$STACKNAME
				- with permission :
					LLNL_User_Policies_$STACKNAME_Base_Policy 

	- KMS key
		$STACKNAME_kms_key 
			- allow LLNL_User_Role_$STACKNAME_Base_Role to use key

	- S3 buckets
		$STACKNAME-data
			- subfolders to store data for each stage of the pipeline : 
				raw/
				preproc/
				interim/
				curated/

		$STACKNAME-code
			- stores all code (Python, Java,...) required to run the Glue jobs
					tahoecommon-xxx.egg
					tahoecomm-xxx.jar
					stackcode_base.zip
					stackcode_nvd.zip
					stackcode_mitre.zip
					stackcode_docex.zip



		$STACKNAME-tmp
			- stores all runtime temp files 
			- subfolders :
				jobs/
				athena-query-results/
				logs/


	- Glue Data Catalog Databases
		- database location = s3://tahoe-w1-tang41-db
		$STACKNAME_db_raw
		$STACKNAME_db_interim
		$STACKNAME_db_curated


## Datasource Stacks
- 1 stack per data source
	- DATASOURCESTACKNAME = $STACKNAME_$DATASOURCE
						   = tahoe_w2_tang41_MITRE

	- a data source can have multiple data types :
		DATASOURCE         DATATYPE
		-----------------------------
		NVD					CVE
							CPE-match
							Vendor
		MITRE				CWE
							ATTACK			



### For each Datatype
1. Stage Retrieval
	SNS topic :
		$DATASOURCESTACKNAME_$DATETYPE_retrieval_done_topic
	EventBridge Rule : 
		$DATASOURCESTACKNAME_$DATETYPE_retrieval_trigger
		- to trigger Lambda on intervals
	Lambda :
		$DATASOURCESTACKNAME_$DATETYPE_retrieval_lambda
		- calls the below :
			Step Function :
				$DATASOURCESTACKNAME_$DATETYPE_retrieval_sf
				- state machine with states :
					Download :
						- calls Glue Python job :
							$DATASOURCESTACKNAME_$DATETYPE_retrieval_glue
								- retrieves the data from external sites
								- stores raw data into raw/ folder
					Success :
						- puts msg onto $DATASOURCESTACKNAME_$DATETYPE_retrieval_done_topic
					Faliure :
						- log error 



2. Stage ingest
	- trigger by new msg on $DATASOURCESTACKNAME_$DATETYPE_retrieval_done_topic
	SNS topic :
		$DATASOURCESTACKNAME_$DATETYPE_ingest_done_topic
	Lambda :
		$DATASOURCESTACKNAME_$DATETYPE_ingest_lambda
		- calls the below :
			Step Function :
				$DATASOURCESTACKNAME_$DATETYPE_ingest_sf
				- state machine with states :
					PreProc :
						- calls Glue Python job :
							$DATASOURCESTACKNAME_$DATETYPE_preproc_glue
								- checks raw data in $STACKNAME_data/Raw/$DATASOURCE/$DATATYPES 
								- perform any cleansing/extractions
								- output validated data into $STACKNAME_data/Preproc/$DATASOURCE/$DATATYPES 				
					CrawlRaw :
						- calls crawler :
							$DATASOURCESTACKNAME_$DATETYPE_ingest_crawler_raw
							- crawl the data files from $STACKNAME_data/Preproc/$DATASOURCE/$DATATYPES files 
							- creates raw table on $STACKNAME_db_raw
													
					Relationalize :
						- calls Glue Sparks Python job :
							$DATASOURCESTACKNAME_$DATETYPE_ingest_relationalize_glue
							- Clears all file on the Interim folder $STACKNAME_data/Interim/$DATASOURCE/$DATATYPES
							- output the new Interim parquet data files into folder $STACKNAME_data/Interim/$DATASOURCE/$DATATYPES
					CrawlInterim :
						- calls crawler
							$DATASOURCESTACKNAME_$DATETYPE_ingest_crawler_interim
							- crawl the data files on $STACKNAME_data/Interim/$DATASOURCE/$DATATYPES  
							- creates interim tables on $STACKNAME_db_interim
							
					Success :
						- puts msg onto $DATASOURCESTACKNAME_$DATETYPE_ingest_done_topic
					Faliure :
						- log error 

## Consumer Curation Stacks
- 1 stack per consumer
	- CONSUMERSTACKNAME = $STACKNAME_$CONSUMER
						= tahoe_w2_tang41_docex

1. Stage Curation
	- trigger by new msg from 1+ ingestion done topics

	SNS topic :
		$CONSUMERSTACKNAME_curation_done_topic
	Lambda :
		$CONSUMERSTACKNAME_curation_lambda
		- calls the below :
			Step Function :
				$CONSUMERSTACKNAME_curation_sf
				- state machine with states :
					Curate_1 :
						- calls Glue Databrew job :
							$CONSUMERSTACKNAME_curate1_databrew
					Curate_2 :
						- calls Glue Databrew job :
							$CONSUMERSTACKNAME_curate2_databrew
					Success :
						- puts msg onto $CONSUMERSTACKNAME_curation_done_topic
					Faliure :
						- log error 

# Deployment  

# Source Repo Structure
1. tahoe/
		bin/
			tahoe-deploy.sh
		build/
		dist/
			- creates the following at build time :
				common-0.1-py2.7.egg
				stack_code_base.zip
				stack_code_nvd.zip
				stack_code_mitre.zip
				stack_code_docex.zip

		src/
			common/	- common libraries used in the Glue jobs
				python/
					setup-common.py 
				java/
			stacks/
				base/		
					base_lambda.py
				datasource/
					nvd/
						cve/
							preproc_glue.py
					mitre/
						cwe/
						attack/
				consumer/
					docex/
						DataBrew Job recipies
						curation stepfunction JSON
					pnnl/


# Build

## Package the .egg - Python 2 modules for use in glue jobs
- Ref :  https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html#python-shell-supported-library

- generates an .EGG package
cd tahoe/src/python/
rm -rf build/ dist/
python setup-common.py bdist_egg

- Glue Python jobs must be create with the following parameter
--extra-py-files s3://$STACKNAME_code/python/common-0.1-py2.7.egg

## Zip the code for each stacks
cd tahoe/src/stacks
	zip -r stackcode_base.zip base
	zip -r stackcode_nvd.zip  nvd


# Deployment  
1. Deployment script
tahoe/bin/tahoe-deploy.sh [COMMAND] [STACK] [Deployment Properties file]
	where
		COMMAND :  
			deploy - deploy the STACK
			destroy - destroys this STACK

		STACK :
			SHARE - the share stack
			BASE - the base stack
			NVD - the NVD datasource stack
			MITRE - the MITRE datasoure stack
			DOCEX
			...

2. Push all the dist artifacts into 
	$STACKNAME_code

# Test Deployments
PROJECT=tahoe
REGION=w1
DEPLOYMENT=tang41

STACKNAME=tahoe_w1_tang41

IAM
	LLNL_User_Policies_tahoe_w1_tang41_Base_Policy 
	LLNL_User_Role_tahoe_w1_tang41_Base_Role
	LLNL_User_Group_tahoe_w1_tang41
	LLNL_User_tahoe_w1_tang41
	
KMS
	tahoe_w1_tang41_kms_key

S3
	tahoe-w1-tang41-data
	tahoe-w1-tang41-code
	tahoe-w1-tang41-tmp


Glue DB
	tahoe-w1-tang41-db


Datasource
	TEST - 
		tahoe_w1_tang41_TEST_crawl_raw

	MITRE
		CWE
			tahoe_w1_tang41_MITRE_CWE_retrieval_done_topic
			tahoe_w1_tang41_MITRE_CWE_ingest_done_topic

# Repo Structure

* **base/** The base folder contains packages, lambda functions, and apis shared among all datasources.
	* **api/** API handler function and related packages
	* **common/** Shared glue library with custom helper functions 
	* **functions/** Shared lambda functions used in various step functions
	* **wheelhouse/** External shared libraries that will be compiled at build time and added to a wheelhouse which allows the use of external pip packages in glue
* **bin/** The bin folder contains scripts to help create file structure for adding new datasources (TODO)
* **consumer/** Contains custom code that consumers use in their pipelines
	* **consumer** Name of the consumer
		* **etl/** Etl jobs for transforming consumer
* **datasources/** Contains custom code that consumers use in their pipelines
	* **datasource** Name of the datasource
		* **download/** Custom loading for datasource
		* **etl/** Transformations required for interim output
		* **validation/** Validation and preprocssing
* **tahoe_infrastructure/** Contains CDK deployment stacks and resources
	* **app.py** Entry point for CDK to deploy stacks
	* **cdk.json** CDK parameters for different deployments
	* **tahoe_infrastructure/** Stack definitions and classes
		EventBridge
			tahoe_w1_tang41_MITRE_CWE_retrieval_rule
		Lambda
			tahoe_w1_tang41_MITRE_CWE_retrieval_lambda
		StepFunction
			tahoe_w1_tang41_MITRE_CWE_retrieval_sf
