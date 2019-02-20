#!/usr/bin/env python
"""
Tests sqs connection methods

Update the SQS_CONN_PARAMS to test
"""
import sys
import unittest
import logging


sys.path.insert(0, '..')
from service_clients.aws.sqs_client import SQSClient


SQS_CONN_PARAMS = {
    'access_key_id': '<test-access-key-id>',
    'secret_access_key': '<test-secret-access-key>',
    'aws_region': 'ca-central-1',
    'queue_url': '<test-queue-url>'
}

S3_EVENT_MESSAGE = \
    '{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":' \
    '"2017-11-02T20:46:02.498Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":' \
    '"AWS:AIDAJHTJFFPZ6JYIXFBGK"},"requestParameters":{"sourceIPAddress":"54.200.43.31"},' \
    '"responseElements":{"x-amz-request-id":"9FEAE30337E9E20B","x-amz-id-2":' \
    '"ePCkzMSQgwXGuBUClOGnXto0xgh729x61BY99rS++jkmyjFUVKErNAQzLgBQd6i/VragX4HqoOw="},"s3":' \
    '{"s3SchemaVersion":"1.0","configurationId":"DeliverToStagingFeedProcessor","bucket":' \
    '{"name":"us-west-2-feeds-uat","ownerIdentity":{"principalId":"A1YTN9GI8TG7SZ"},' \
    '"arn":"arn:aws:s3:::us-west-2-feeds-uat"},"object":{"key":' \
    '"home/ftp/1/file.txt-1509655557","size":674911,' \
    '"eTag":"939cfcaf3d04deb439927640aa2e0f99","sequencer":"0059FB840A5CF6BEB4"}}}]}'

# TODO - add all tests

# # Create some messages
# client.send_message(body={'test': 'body'})
# client.send_message(body={'test': 'body'})
#
# msgs = client.get_messages(num_messages=2)
#
# assert isinstance(msgs, list)
# assert msgs
#
# assert len(msgs) >= 1, len(msgs)  # Could be 1 or 2
# assert isinstance(msgs[0], dict), type(msgs[0])
#
# is_deleted = [client.delete_message(msg) for msg in msgs]
# assert all(is_deleted)
#



class TestSQSClient(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')
        self.sqs_client = SQSClient(SQS_CONN_PARAMS, conn_retries=1, msg_wait_seconds=10)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

        # Clean up all messages
        # msgs = True
        # while msgs:
        #     msgs = self.sqs_client.get_messages(msg_wait_seconds=1)
        #     [self.sqs_client.delete_message(msg) for msg in msgs]

    def shortDescription(self):
        return None

        # Test start below

    def test_connection1(self):
        conn = self.sqs_client.client
        assert conn

    # def test_message1(self):
    #     """
    #     Test sending a message
    #     """
    #     sqs_message = self.sqs_client.send_message('{"test":"this is a test message"}')
    #     assert str(type(sqs_message)) == "<class 'boto.sqs.message.Message'>"
    #
    # def test_message2(self):
    #     """
    #     Test sending a message
    #     that resembles a feed queue msg
    #     This will produce an S3 error (expected) since file
    #     is not going to be found under S3
    #     """
    #     self.sqs_message = self.sqs_client.send_message(S3_EVENT_MESSAGE)
    #     assert str(type(self.sqs_message)) == "<class 'boto.sqs.message.Message'>"
    #
    # def test_message3(self):
    #     """
    #     Test receiving a message
    #     """
    #     messages = self.sqs_client.get_messages(msg_wait_seconds=1)
    #     assert isinstance(messages, list)
    #
    # def test_message4(self):
    #     """
    #     Test deleting a message
    #     """
    #     assert self.sqs_client.delete_message(self.sqs_message)
