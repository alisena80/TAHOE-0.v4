from constructs import Construct
import aws_cdk.aws_sns as sns
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig
import aws_cdk.aws_sns_subscriptions as subscriptions


class BaseSns(TahoeConstruct):
    """
    A class to represent an  encapsulation of all the base sns to enable pipeline operations.
     

    Attributes
    ----------
    email_sns_topic : sns.Topic
        Email sns topic topic
    query_sns : sns.Topic
        Query sns topic
    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig):
        """
        Constructs all base sns 

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
        """
        super().__init__(scope, id, build_context)
        self.email_sns_topic = sns.Topic(self, "emailSns",
                                         display_name=self.build_context.sns_name(
                                             "email"),
                                         topic_name=self.build_context.sns_name("email"))
        self.email_sns_topic.add_subscription(
            topic_subscription=subscriptions.EmailSubscription(email_address=self.build_context.get_env() + "@llnl.gov"))
        self.query_sns = sns.Topic(self, "querySns",
                                         display_name=self.build_context.sns_name(
                                             "queries"),
                                         topic_name=self.build_context.sns_name("queries"))
