from scripts.uploaders.base_uploader import Uploader


class FirebaseUploader(Uploader):
    def __init__(self, firebase, idToken):
        self.firebase = firebase
        self.storage = firebase.storage()
        self.idToken = idToken

    def upload_file(self, file_name, object_name=None):
        name = os.basename(file_name)
        if object_name is not None:
            name = os.basename(object_name)
        self.storage.child(name).put(file_name, self.idToken)
        url = self.storage.child(file_name).child(name).get_url(None)

    def upload_image(self, image, object_name):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            with open(image, "rb") as f:
                response = s3_client.upload_fileobj(f, self.bucket_name, object_name)
                print(response)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_buffer(self, buffer, object_name):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # Upload the