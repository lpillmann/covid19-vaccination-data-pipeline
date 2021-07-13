import configparser
import json
import os
import time

import boto3


AWS_KEY = os.environ.get("UDACITY_AWS_KEY")
AWS_SECRET = os.environ.get("UDACITY_AWS_SECRET")
AWS_REGION = os.environ.get("UDACITY_AWS_REGION")

CLUSTER_HOST = os.environ.get("UDACITY_REDSHIFT_HOST")
CLUSTER_DB_NAME = os.environ.get("UDACITY_REDSHIFT_DB_NAME")
CLUSTER_DB_USER = os.environ.get("UDACITY_REDSHIFT_DB_USER")
CLUSTER_DB_PASSWORD = os.environ.get("UDACITY_REDSHIFT_DB_PASSWORD")
CLUSTER_DB_PORT = os.environ.get("UDACITY_REDSHIFT_DB_PORT")


def create_iam_resourceole(config):
    iam = boto3.client(
        "iam", aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET
    )

    print("1.1 Creating a new IAM Role")
    try:
        dwh_role = iam.create_role(
            Path="/",
            RoleName=config["CLUSTER_SPECS"]["IAM_ROLE_NAME"],
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )

    except iam.exceptions.EntityAlreadyExistsException as e:
        print("Role already exists. Getting info from IAM")
        dwh_role = iam.get_role(RoleName=config["CLUSTER_SPECS"]["IAM_ROLE_NAME"])

    print("1.2 Attaching Policy")
    iam.attach_role_policy(
        RoleName=config["CLUSTER_SPECS"]["IAM_ROLE_NAME"],
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("1.3 Get the IAM role ARN")
    role_arn = dwh_role["Role"]["Arn"]
    print("Done.", role_arn)

    return role_arn


def create_cluster(config, role_arn):
    def get_cluster_properties():
        cluster_properties = redshift.describe_clusters(
            ClusterIdentifier=cluster_config["CLUSTER_IDENTIFIER"]
        )["Clusters"][0]
        return cluster_properties

    redshift = boto3.client(
        "redshift",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    cluster_config = config["CLUSTER_SPECS"]

    print("2.1 Creating cluster")
    try:
        redshift.create_cluster(
            # parameters for hardware
            ClusterType=config["CLUSTER_SPECS"]["CLUSTER_TYPE"],
            NodeType=config["CLUSTER_SPECS"]["NODE_TYPE"],
            NumberOfNodes=int(config["CLUSTER_SPECS"]["NUM_NODES"]),
            # parameters for identifiers & credentials
            ClusterIdentifier=config["CLUSTER_SPECS"]["CLUSTER_IDENTIFIER"],
            DBName=CLUSTER_DB_NAME,
            MasterUsername=CLUSTER_DB_USER,
            MasterUserPassword=CLUSTER_DB_PASSWORD,
            # parameter for role (to allow s3 access)
            IamRoles=[role_arn],
        )

    except redshift.exceptions.ClusterAlreadyExistsFault as e:
        print("Cluster already exists")

    cluster_properties = get_cluster_properties()
    while cluster_properties["ClusterStatus"] != "available":
        print("Waiting for cluster to start...")
        print("Status:", cluster_properties["ClusterStatus"])
        time.sleep(5)
        cluster_properties = get_cluster_properties()

    print("Cluster available")
    return {
        "endpoint": cluster_properties["Endpoint"]["Address"],
        "role_arn": cluster_properties["IamRoles"][0]["IamRoleArn"],
        "vpc_id": cluster_properties["VpcId"],
    }


def configure_security_group(config, vpc_id):
    ec2 = boto3.resource(
        "ec2",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    print("3.1 Configure security group")
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        port = int(CLUSTER_DB_PORT)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=port,
            ToPort=port,
        )

    except Exception as e:
        if "InvalidPermission.Duplicate" in str(e):
            print("Security group rule already exists")
        else:
            print(e)


def main():
    config = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config.read_file(open(os.path.join(dir_path, "redshift.cfg")))

    role_arn = create_iam_resourceole(config)
    cluster_properties = create_cluster(config, role_arn)
    configure_security_group(config, cluster_properties["vpc_id"])

    print(f"Done. Cluster created and available: {cluster_properties['endpoint']}")


if __name__ == "__main__":
    main()
