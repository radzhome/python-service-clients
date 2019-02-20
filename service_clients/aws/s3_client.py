from __future__ import unicode_literals
"""
S3 bucket CRUD operations core module
"""
import logging
import time
import warnings

import boto3
import botocore
from botocore.client import Config



class S3Client:  # pragma: no cover
    """
    S3 class encapsulates uploading,
    downloading & other s3 file ops and handling errors
    This is not covered in unit test test coverage,
    but in integration tests since its an external process
    """
    S3_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'  # Not used

    RECONNECT_SLEEP_SECS = 0.5
    CONN_RETRIES = 10
    CONN_CONFIG = Config(connect_timeout=5, retries={'max_attempts': 0})

    def __init__(self, config, reconnect_sleep_secs=RECONNECT_SLEEP_SECS, conn_retries=CONN_RETRIES):
        """
        Load config from passed params or override with defaults
        :param config: dict, config with access_key_id, secret_access_key, bucket name
        :return: None
        """
        self.config = config
        self.bucket_name = self.config['bucket_name']
        self.access_key_id = self.config['access_key_id']
        self.secret_access_key = self.config['secret_access_key']
        self.aws_region = self.config['aws_region']

        self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs
        self.CONN_RETRIES = conn_retries

        self.connection_attempt = 0
        self.connection = None
        self.bucket = None
        self.connect()

    def connect(self):
        """
        Creates object connection to the designated region (self.boto.cli_region).
        The connection is established on the first call for this instance (lazy) and cached.
        :return: None
        """
        try:
            self.connection_attempt += 1

            self.connection = boto3.resource('s3', region_name=self.aws_region,
                                             aws_access_key_id=self.access_key_id,
                                             aws_secret_access_key=self.secret_access_key,
                                             config=self.CONN_CONFIG)
            self._get_bucket()
        except Exception as e:
            logging.exception("S3Client.connect failed with params {}, error {}".format(self.config, e))
            if self.connection_attempt >= self.CONN_RETRIES:
                raise

    def _get_bucket(self):
        """
        Uses S3 Connection and return connection to queue
        S3 used for getting the listing file in the SQS message
        :return: None
        """
        try:
            self.bucket = self.connection.Bucket(name=self.bucket_name)
        except Exception as e:
            # I.e. gaierror: [Errno -2] Name or service not known
            logging.exception("S3Client.get_bucket unable to get bucket {}, error {}".format(self.bucket_name, e))
            raise

    def list(self):
        """
        List contents of a bucket
        :return: list of s3.ObjectSummary
        """
        return list(self.bucket.objects.all())

    def read(self, key):
        """
        Get bucket key value, return contents
        Get contents of a file from S3
        :param key: str, bucket key filename
        :return: str, contents of key
        """
        try:
            obj = self.connection.Object(self.bucket_name, key)
            contents = obj.get()['Body'].read().decode('utf-8')
        except Exception as e:  # Retry in-case we have a connection error
            logging.exception("S3Client.read failed for key {}, error {}".format(key, e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            contents = self.read(key)

        return contents

    def write(self, key, contents):
        """
        Create bucket key from string
        Write content to a file in S3
        :param contents: str, contents to save to a file
        :param key: str, bucket key filename
        :return: dict, output
        """
        output = response = None
        try:
            response = self.connection.Object(self.bucket_name, key).put(Body=contents)
            output = {
                'file_name': key,
                # 'is_new': not k.exists(),
            }
        except Exception as e:
            logging.exception("S3Client.write failed for key {}, error {}, response {}".format(key, e, response))

        return output

    def upload(self, key, origin_path):
        """
        Create bucket key from filename
        Upload a file to S3 from origin file
        :param origin_path: str, path to origin filename
        :param key: str, bucket key filename
        :return: bool, success
        """
        try:
            file_body = open(origin_path, 'rb')
            self.connection.Bucket(self.bucket_name).put_object(Key=key, Body=file_body)
        except Exception as e:
            logging.exception("S3Client.upload failed for key {}, error {} ".format(key, e))

        return True

    def download(self, key, destination):
        """
        Get key
        Download a file from S3 to destination
        :param destination: str, path to local file name
        :param key: str, bucket key filename
        :return: bool, success
        """
        result = True
        try:
            self.bucket.download_file(key, destination)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logging.error("S3Client.download bucket missing key file {}".format(key))
            else:
                raise
        except Exception as e:
            logging.warning("S3Client.download failed for key {} to {}, error {}, retrying".format(key, destination, e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            result = self.download(key, destination)

        return result

    def remove(self, keys):
        """
        Deletes the given keys from the given bucket.
        :param keys: list, list of key names
        :return: bool, success
        """
        logging.warning("S3Client.remove deleting keys {}".format(keys))
        objects = [{'Key': key} for key in keys]
        self.bucket.delete_objects(Delete={'Objects': objects})
        return True


if __name__ == "__main__":  # pragma: nocover

    conf = {'access_key_id': '<test key id>',
            'secret_access_key': '<test access key>',
            'aws_region': 'ca-central-1',
            'bucket_name': 'aws-web-distro'}

    s3_client = S3Client(config=conf)

    print(s3_client.list())

    # s3_client.upload('test2.jpg', '/Users/rad/Desktop/test.jpg')

    print(s3_client.read('readonly-access/readonly.txt'))

    # print(s3_client.write('new-test-key.txt', 'this is some data'))

    print(s3_client.remove(keys=['test.jpg', 'test2.jpg']))

    # s3_client.download('new-test-key.txt', 'my_local_image.jpg')
