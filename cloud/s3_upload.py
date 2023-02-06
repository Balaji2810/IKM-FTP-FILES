import os
from boto3.session import Session
from boto3.s3.transfer import S3Transfer
import logging
import configparser

config = configparser.ConfigParser()
config.read("secret.ini")


def get_s3_client():
    session = Session()
    client = session.client(
        "s3",
        region_name=config["S3"]["region"],
        endpoint_url=f'https://{config["S3"]["region"]}.digitaloceanspaces.com',
        aws_access_key_id=config["S3"]["access_key"],
        aws_secret_access_key=config["S3"]["secret_access_key"],
    )
    return client


def upload_to_s3(filename):
    client = get_s3_client()
    transfer = S3Transfer(client)

    local_path = config["LOCAL"]["path"] + "/" + filename

    client.delete_object(Bucket=config["S3"]["space"], Key=filename)
    print(f'Removed {filename} from {config["S3"]["space"]} S3 space')
    logging.debug(f'Removed {filename} from {config["S3"]["space"]} S3 space\n')

    print(f"Going to upload local file {local_path}")
    logging.debug(f"Going to upload local file {local_path}")

    transfer.upload_file(local_path, config["S3"]["space"], filename)
    client.put_object_acl(ACL="public-read", Bucket=config["S3"]["space"], Key=filename)
    print(f"Uploaded local file {local_path} as {filename}")
    logging.debug(f"Uploaded local file {local_path} as {filename}")

    file_url = f'https://{config["S3"]["space"]}.{config["S3"]["region"]}.digitaloceanspaces.com/{filename}'

    print(
        f'File {local_path} is available in {config["S3"]["space"]} S3 space at:'
        + file_url
    )
    logging.debug(
        f'File {local_path} is available in {config["S3"]["space"]} S3 space at:'
        + file_url
        + "\n"
    )

    remove_local_content(local_path)
    print(f"Cleaned local file {local_path}. Done")
    logging.debug(f"Cleaned local file {local_path}. Done\n")

    return file_url


def remove_local_content(path):
    if os.path.exists(path):
        os.remove(path)
