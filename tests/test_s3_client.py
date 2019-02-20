#!/usr/bin/env python
"""
Tests s3 connection methods

Update the S3_CONN_PARAMS to test
"""
import sys
import unittest
import logging
import warnings
import os

sys.path.insert(0, '..')
from service_clients.aws.s3_client import S3Client


# Update bucket_name and credentials
S3_CONN_PARAMS = {
    'access_key_id': '<test-access-key-id>',
    'secret_access_key': '<test-secret-access-key>',
    'aws_region': 'ca-central-1',
    'bucket_name': '<test-bucket>'
}

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def remove_file(file_path):
    """
    Delete a file
    :param file_path: str, filename with path
    :return: bool, success
    """
    try:
        os.remove(file_path)
        return True
    except OSError:
        return False


class TestS3Client(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')

        # Silence some of the errors associated with long lived connections, unclosed file buffers
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*file")

        self.s3_client = S3Client(S3_CONN_PARAMS)

        self.key_name = 'new-test-key23.txt'
        self.key_contents = 'this is some data'

        self.test_jpg_filename = os.path.join(TEST_DIR, 'assets', 'test.jpg')
        self.test_txt_filename = os.path.join(TEST_DIR, 'assets', 'test.txt')
        self.jpg_key_name = 'test.jpg'
        self.txt_key_name = 'test.txt'

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    def shortDescription(self):
        return None

    def test_connection1(self):
        conn = self.s3_client.connection
        assert conn

    def test_list1(self):
        list_result = self.s3_client.list()
        assert isinstance(list_result, list)

    def test_s3_file_write1(self):
        """
        Test writing to s3
        """
        result = self.s3_client.write(key=self.key_name, contents=self.key_contents)
        assert isinstance(result, dict)
        assert 'file_name' in result

    def test_s3_file_write2(self):
        """
        Test read written file from s3
        """
        result = self.s3_client.read(self.key_name)
        print(result)
        assert result == self.key_contents, "Got {}, {}".format(result, type(result))

    def test_s3_file_write3(self):
        """
        Test delete written file from s3
        """
        delete_result = self.s3_client.remove(keys=[self.key_name, ])
        assert delete_result

    def test_remove1(self):
        """
        Test delete from s3
        """
        delete_result = self.s3_client.remove(keys=['non-existent-1', 'non-existent-2'])
        assert delete_result  # Does not care if files are not there, still success

    def test_s3_upload1(self):
        """
        Test upload jpg
        """
        result = self.s3_client.upload(key=self.jpg_key_name, origin_path=self.test_jpg_filename)
        assert result is True

    def test_s3_upload2(self):
        """
        Test download uploaded file from s3 to file
        """
        destination = os.path.join(TEST_DIR, 'assets', 'test_dl.jpg')
        result = self.s3_client.download(key=self.jpg_key_name, destination=destination)
        assert result
        assert os.path.exists(destination)

        # Cleanup
        remove_file(destination)
        self.s3_client.remove(keys=[self.jpg_key_name, ])

    def test_s3_upload3(self):
        """
        Test upload txt
        """
        result = self.s3_client.upload(key=self.txt_key_name, origin_path=self.test_txt_filename)
        assert result is True

    def test_s3_upload4(self):
        """
        Test download uploaded file from s3 to file
        """
        destination = os.path.join(TEST_DIR, 'assets', 'test_dl.txt')
        result = self.s3_client.download(key=self.txt_key_name, destination=destination)
        assert result
        assert os.path.exists(destination)

        # Test contents of file
        result = self.s3_client.read(self.txt_key_name)
        result = result.split('\n')
        assert result == ['this', 'is a file', 'upload test', '']

        # Cleanup
        remove_file(destination)
        self.s3_client.remove(keys=[self.txt_key_name, ])


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
