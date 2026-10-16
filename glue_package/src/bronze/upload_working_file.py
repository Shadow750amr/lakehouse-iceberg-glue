import logging
import boto3
from botocore.exceptions import ClientError
from utils.timezone import get_time_zone
import os

year = get_time_zone()["year"]
month = get_time_zone()["month"]
day= get_time_zone()["day"]

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == "__main__":
    upload_file("inventory.csv",f"erp",f"raw/year={year}/month={month}/day={day}/inventory.csv")