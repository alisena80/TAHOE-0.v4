from constructs import Construct
from aws_cdk import aws_glue as glue
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.build_config import BuildConfig
import aws_cdk.aws_secretsmanager as secret


class BaseConnections(TahoeConstruct):
    """
    A class to represent glue connection constructs
     

    Attributes
    ----------
    cyhy_connection : glue.CfnConnection
        Mongo connection details for cyhy
    redshift_connection : glue.CfnConnection
        Redshift connection details

    """

    def __init__(self, scope: Construct, id: str, build_context: BuildConfig, vpc):
        """
        Constructs all connections for glue

        Parameters
        ----------
            scope : Construct
                construct scope
            id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            vpc : ec2.Vpc
                VPC configuration to link connections
        """
        super().__init__(scope, id, build_context)
        if self.build_context.get_credential("docdb_secret"):
            self.cyhy_connection = self.mongo_connection(self.build_context.glue_name(
                "cyhy", "connection"), "cyhy",
                secret.Secret.from_secret_name_v2(self.build_context.get_credential("docdb_secret")), vpc,
                "docdb_security_group")
        if self.build_context.get_credential("redshift_secret"):
            self.redshift_connection = self.jdbc_connection(
                self.build_context.glue_name("redshift", "connection"), "dev", vpc, "redshift_security_group")

    def mongo_connection(self, name, database, secret: secret.Secret, vpc, security_group_secret):
        connection_properties = {
            "CONNECTION_URL": "mongodb://" + secret.secret_value_from_json(
                "host").to_string() + ":" + secret.secret_value_from_json(
                "port").to_string() + "/" + database,
            "JDBC_ENFORCE_SSL": secret.secret_value_from_json(
                "ssl").to_string(),
            "USERNAME": secret.secret_value_from_json(
                "username").to_string(),
            "PASSWORD": secret.secret_value_from_json(
                "password").to_string()
        }

        connection_input = glue.CfnConnection.ConnectionInputProperty(name="cyhy_connection",
                                                                      connection_type="MONGODB",
                                                                      connection_properties=connection_properties,
                                                                      physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                                                                          subnet_id=
                                                                          vpc.public_subnets[
                                                                              0].subnet_id,
                                                                          availability_zone=
                                                                          vpc.public_subnets[
                                                                              0].availability_zone,
                                                                          security_group_id_list=[
                                                                              self.build_context.get_credential(
                                                                                  security_group_secret) if self.build_context.get_credential(
                                                                                  security_group_secret) else secret.secret_value_from_json(
                                                                                  "security_group").to_string()]))

        return glue.CfnConnection(self, name, catalog_id=self.build_context.get_account(),
                                  connection_input=connection_input)

    def jdbc_connection(self, name, database, secret: secret.Secret, vpc, security_group_secret):
        connection_properties = {
            "JDBC_CONNECTION_URL": f"jdbc://{secret.secret_value_from_json('host').to_string()}"
                                   f":{secret.secret_value_from_json('port').to_string()}/{database}",
            "JDBC_ENFORCE_SSL": "false",
            "USERNAME": secret.secret_value_from_json(
                "username").to_string(),
            "PASSWORD": secret.secret_value_from_json(
                "password").to_string()
        }

        connection_input = glue.CfnConnection.ConnectionInputProperty(name=name,
                                                                      connection_type="JDBC",
                                                                      connection_properties=connection_properties,
                                                                      physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                                                                          subnet_id=self.vpc.public_subnets[0].subnet_id,
                                                                          availability_zone=self.vpc.public_subnets[0].availability_zone,
                                                                          security_group_id_list=[
                                                                              self.build_context.get_credential(
                                                                                  security_group_secret) if self.build_context.get_credential(
                                                                                  "security_group_secret") else secret.secret_value_from_json(
                                                                                  "security_group").to_string()]))

        return glue.CfnConnection(self, name, catalog_id=self.build_context.get_account(),
                                  connection_input=connection_input)
