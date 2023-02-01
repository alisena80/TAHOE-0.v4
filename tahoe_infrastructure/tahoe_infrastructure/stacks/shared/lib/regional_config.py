import aws_cdk.aws_docdb as ddb
import aws_cdk.aws_redshift_alpha as rdb
import aws_cdk.aws_iam as iam


class SandboxConfig():
    def __init__(self, redshift, redshift_role, docdb):
        self.redshift_cluster: rdb.Cluster = redshift
        self.redshift_role: iam.Role = redshift_role
        self.docdb_cluster: ddb.DatabaseCluster = docdb
