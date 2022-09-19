import boto3
import os
import logging
import boto3
from botocore.exceptions import ClientError
import os

from scripts.uploaders.base_uploader import Uploader


def upload_file(file_name, object_name=None):
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
        with open(file_name, "rb") as f:
            response = s3_client.upload_fileobj(f, "doubtech-aiart", object_name)
            print(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True

class AmazonS3Uploader(Uploader):
    def __init__(self, bucket_name, access_key=None, secret_key=None):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        if access_key is None:
            self.s3 = boto3.client('s3')
        else:
            self.s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        print(f"[AWS] Uploading file: {file_name} to {object_name}")

        try:
            with open(file_name, "rb") as f:
                response = self.s3.upload_fileobj(f, self.bucket_name, object_name)
                url = f"https://{self.bucket_name}.s3.amazonaws.com/{object_name}"
                print("[AWS] Uploaded file: " + url)
                return url
        except ClientError as e:
            logging.error(e)
            return None

    def get_url(self, object_name):
        return f"https://{self.bucket_name}.s3.amazonaws.com/{object_name}"

    def upload_image(self, image, object_name):
        print("TODO")


    def upload_buffer(self, buffer, object_name):
        print("TODO")