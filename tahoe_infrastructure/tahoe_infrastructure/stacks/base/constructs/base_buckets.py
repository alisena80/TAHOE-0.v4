from cmath import log
from aws_cdk import RemovalPolicy
import aws_cdk
from constructs import Construct
import aws_cdk.aws_kms as kms
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ec2 as ec2
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig


class BaseBuckets(TahoeConstruct):
    """
    A class to represent an  encapsulation of all the base base buckets to enable pipeline operations.
     

    Attributes
    ----------
    raw_bucket : s3.Bucket
        Bucket to support raw datasource files
    preprocess_bucket: s3.Bucket
        Bucket to support reformatting of raw files to enable safe crawling/transformation
    interim_bucket : s3.Bucket
        Bucket to support relationalized/transformed datasource files
    curated_bucket: s3.Bucket
        Bucket to support final consumer files
    self.bootstrap_bucket: s3.Bucket
        Asset bucket created by cdk to house assets
    self.wheelhouse_bucket: ss3.Bucket
        Bucket to support 3rd party pypi packages in glue

    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, kms_target: kms.Key, vpc: ec2.Vpc):
        """
        Constructs all base buckets

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            kms_target : kms.Key
                Kms key to encrypt data with
        """
        super().__init__(scope, id, build_context)

        delete_bucket = self.build_context.get_optional("auto_delete_buckets") if self.build_context.get_optional("auto_delete_buckets") else False

        self.raw_bucket = s3.Bucket(self,
                                    "rawBucket", 
                                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                    bucket_name=self.build_context.s3_name(
                                        "raw"),
                                    encryption=s3.BucketEncryption.KMS,
                                    encryption_key=kms_target,  auto_delete_objects=delete_bucket, removal_policy=RemovalPolicy.DESTROY)

        self.interim_bucket = s3.Bucket(self, "interimBucket", block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                        bucket_name=self.build_context.s3_name("interim"), encryption=s3.BucketEncryption.KMS,
                                        encryption_key=kms_target,  auto_delete_objects=delete_bucket, removal_policy=RemovalPolicy.DESTROY)

        self.curated_bucket = s3.Bucket(self, "curatedBucket", block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                        bucket_name=self.build_context.s3_name("curated"), encryption=s3.BucketEncryption.KMS,
                                        encryption_key=kms_target, auto_delete_objects=delete_bucket, removal_policy=RemovalPolicy.DESTROY)

        self.preprocess_bucket = s3.Bucket(self, "preprocessBucket", block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                           bucket_name=self.build_context.s3_name("preprocess"), encryption=s3.BucketEncryption.KMS,
                                           encryption_key=kms_target,  auto_delete_objects=delete_bucket, removal_policy=RemovalPolicy.DESTROY)

        # imports cdk generated asset bucket
        self.bootstrap_bucket = s3.Bucket.from_bucket_name(
            self, "bootstrapBucket", self.build_context.get_asset_bucket(True))

        if vpc:
            # houses 3rd party assets
            self.wheelhouse_bucket = s3.Bucket(self, "wheelhouseBucket", block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                            bucket_name=self.build_context.s3_name("wheelhouse"), website_index_document="index.html", auto_delete_objects=delete_bucket, removal_policy=RemovalPolicy.DESTROY)
            
            wheelhouse_policy = iam.PolicyStatement(actions=["s3:*"], principals=[iam.StarPrincipal()], conditions={"StringEquals": {
                        "aws:SourceVpc": vpc.vpc_id
                    }}, resources=[self.wheelhouse_bucket.bucket_arn, self.wheelhouse_bucket.bucket_arn + "/*"])
            wheelhouse_policy_attachment = self.wheelhouse_bucket.add_to_resource_policy(wheelhouse_policy)
            if(not wheelhouse_policy_attachment.statement_added):
                self.logger.critical("Wheelhouse policy attachment failed")
        
