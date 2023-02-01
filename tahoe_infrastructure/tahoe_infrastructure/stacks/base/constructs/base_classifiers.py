from constructs import Construct
from aws_cdk import aws_glue as glue
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig


class BaseClassifiers(TahoeConstruct):
    """
    A class to represent an  encapsulation of all crawler classifers to enable sharing between all datasources/consumers.
     

    Attributes
    ----------
    json_array : glue.CfnClassifier
        Classifier to nest into base json array once

    """
    def __init__(self, scope: Construct, id: str, build_context: BuildConfig):
        """
        Constructs all base classifiers

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
        self.json_array = glue.CfnClassifier(scope, "jsonArray", json_classifier=glue.CfnClassifier.JsonClassifierProperty(
            json_path="$[*]", name=self.build_context.glue_name("base", "JsonArray")))
