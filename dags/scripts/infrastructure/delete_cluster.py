import configparser
import os

import boto3


AWS_KEY = os.environ.get("UDACITY_AWS_KEY")
AWS_SECRET = os.environ.get("UDACITY_AWS_SECRET")
AWS_REGION = os.environ.get("UDACITY_AWS_REGION")


def delete_cluster(config):
    redshift = boto3.client(
        "redshift",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    cluster_config = config["CLUSTER_SPECS"]

    print("Deleting cluster")
    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_config["CLUSTER_IDENTIFIER"],
            SkipFinalClusterSnapshot=True,
        )
    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config.read_file(open(os.path.join(dir_path, "redshift.cfg")))

    delete_cluster(config)

    aws_console_url = f"https://{AWS_REGION}.console.aws.amazon.com/redshiftv2/home?region={AWS_REGION}#clusters"
    print(f"Please confirm deletion using AWS Console:\n{aws_console_url}")


if __name__ == "__main__":
    main()
