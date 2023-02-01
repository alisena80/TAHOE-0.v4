from constructs import Construct
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
import aws_cdk.aws_s3_notifications as s3n

from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct


class SnsConfig(TahoePipelineConstruct):
    '''
    Creates sns for datasources

    download_done_topic: sns.Topic
        Sns to sgnify completion of download
    ingested_done_topic: sns.Topic
        Sns to signify completion of datasource creation process to create curated
    '''
    def __init__(self, scope: Construct, id: str, build_context, data_prefix, base_context):
        """
        Creates sns config class

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
        """
        super().__init__(scope, id, build_context, base_context, data_prefix)
        self.download_done_topic = sns.Topic(self, data_prefix + "rawTopic",
                                             display_name=self.build_context.sns_name(
                                                 self.data_prefix + "download_done"),
                                             topic_name=self.build_context.sns_name(
                                                 self.data_prefix + "_download_done")
                                             )

        self.ingested_done_topic = sns.Topic(self, data_prefix + "interimTopic",
                                             display_name=self.build_context.sns_name(
                                                 self.data_prefix + "ingested_done"),
                                             topic_name=self.build_context.sns_name(
                                                 self.data_prefix + "_ingested_done")
                                             )
