from typing import List
from xmlrpc.client import boolean
from constructs import Construct
import aws_cdk.aws_glue as glue
import aws_cdk.aws_lakeformation as lakeformation
from stacks.shared.constructs.tahoe_pipeline_construct import TahoePipelineConstruct
from stacks.shared.lib.base_config import BaseConfig
from stacks.shared.lib.build_config import BuildConfig


class EtlDatabaseConfig(TahoePipelineConstruct):
    """

    A class for glue database creation for a datasource.

    Attributes
    ----------
    raw_database : glue.CfnDatabase
        Raw database to track raw schemas
    interim_database : glue.CfnDatabase
        Interim database to track interim schemas
    curated_database : glue.CfnDatabase
        Curated database to track curated schemas

    """

    def __init__(
            self, scope: Construct, id: str, build_context: BuildConfig, data_prefix: str,
            base_context: BaseConfig):
        """

        Create glue database and permissions.

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
        self.raw_database = glue.CfnDatabase(
            self, "raw", catalog_id=self.build_context.get_account(),
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.build_context.glue_name(
                    data_prefix, "database_raw")))

        self.interim_database = glue.CfnDatabase(
            self, "interim", catalog_id=self.build_context.get_account(),
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.build_context.glue_name(
                    data_prefix, "database_interim")))

        self.curated_database = glue.CfnDatabase(
            self, "curated", catalog_id=self.build_context.get_account(),
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.build_context.glue_name(
                    data_prefix, "database_curated")))

        principal = lakeformation.CfnPermissions.DataLakePrincipalProperty(
            data_lake_principal_identifier=self.base_context.get_pipeline_role().role_arn)

        read_only_user_role = lakeformation.CfnPermissions.DataLakePrincipalProperty(
            data_lake_principal_identifier=self.base_context.get_read_only_role().role_arn)

        for x in [self.curated_database, self.raw_database, self.interim_database]:
            read_only_table = ["DESCRIBE", "SELECT"]
            read_only_database = ["DESCRIBE"]
            depends_on = []
            # Permissions to grant pipeline role to modify tables for datasource
            read_only_role_table_permissions = self.table_permissions(
                x, "readOnlyRoletablesPermission", read_only_user_role,
                read_only_table, grantable=False)
            table_permissions = self.table_permissions(
                x, "tablePermission", principal, ["ALL"])

            # Pemissions to grant to pipeline role to modify database for datasource
            read_only_role_database_permissions = self.database_permissions(
                x, "readOnlyRoletdatabasesPermission", read_only_user_role,
                read_only_database, grantable=False)
            database_permissions = self.database_permissions(
                x, "databasePermission", principal, ["ALL"])
            depends_on.extend(
                [read_only_role_table_permissions, table_permissions,
                 read_only_role_database_permissions, database_permissions])
            for permissions in depends_on:
                permissions.add_depends_on(x)

    def database_permissions(
            self, db: glue.CfnDatabase, id: str,
            principal: lakeformation.CfnPermissions.DataLakePrincipalProperty,
            permissions: List[str],
            grantable: boolean = True):
        """

        Create glue database permissions.

        Parameters
        ----------
            db: Construct
                construct scope
            id: str
                construct id
            principal: lakeformation.CfnPermissions.DataLakePrincipalProperty
                Lakeformation principal resource which gets permisisons
            permissions: List[str]
                Permissions to add to database
            grantable: boolean
                Allow pricipal to grant permissions listed in permissions
        """
        return lakeformation.CfnPermissions(
            self, db.database_input.name + id, data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.
                DatabaseResourceProperty(name=db.database_input.name),),
            permissions=permissions, permissions_with_grant_option=permissions
            if grantable else[])

    def table_permissions(
            self, db: glue.CfnDatabase, id: str,
            principal: lakeformation.CfnPermissions.DataLakePrincipalProperty,
            permissions: List[str],
            grantable: boolean = True):
        """

        Create glue table permissions.

        Parameters
        ----------
            db: Construct
                construct scope
            id: str
                construct id
            principal: lakeformation.CfnPermissions.DataLakePrincipalProperty
                Lakeformation principal resource which gets permisisons
            permissions: List[str]
                Permissions to add to database
            grantable: boolean
                Allow pricipal to grant permissions listed in permissions
        """
        return lakeformation.CfnPermissions(
            self, db.database_input.name + id, data_lake_principal=principal,
            resource=lakeformation.CfnPermissions.ResourceProperty(
                table_resource=lakeformation.CfnPermissions.
                TableResourceProperty(
                    database_name=db.database_input.name, table_wildcard={})),
            permissions=permissions, permissions_with_grant_option=permissions
            if grantable else[])
