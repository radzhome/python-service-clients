#!/usr/bin/env python
"""
Tests s3 connection methods

Update the S3_CONN_PARAMS to test
"""
import sys
import unittest
import logging

# TODO - add all tests


#
# s3_client = S3Client(config=conf)
#
# print(s3_client.list())
#
# # s3_client.upload('test2.jpg', '/Users/rad/Desktop/test.jpg')
#
# print(s3_client.read('readonly-access/readonly.txt'))
#
# # print(s3_client.write('new-test-key.txt', 'this is some data'))
#
# print(s3_client.remove(keys=['test.jpg', 'test2.jpg']))
#
# # s3_client.download('new-test-key.txt', 'my_local_image.jpg')


sys.path.insert(0, '..')
from service_clients.aws.s3_client import S3Client


S3_CONN_PARAMS = {
    'access_key_id': '<test-access-key-id>',
    'secret_access_key': '<test-secret-access-key>',
    'aws_region': 'ca-central-1',
    'bucket_name': '<test-bucket>'
}


class TestS3Client(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')
        self.s3_client = S3Client(S3_CONN_PARAMS)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    def shortDescription(self):
        return None

        # Test start below

    def test_connection1(self):
        conn = self.s3_client.connection
        assert conn
