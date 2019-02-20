#!/usr/bin/env python
"""
Tests sqs connection methods

Update the SQS_CONN_PARAMS to test
"""
import sys
import unittest
import logging
import warnings

sys.path.insert(0, '..')
from service_clients.aws.sqs_client import SQSClient


# Update bucket_name and credentials
SQS_CONN_PARAMS = {
    'access_key_id': '<test-access-key-id>',
    'secret_access_key': '<test-secret-access-key>',
    'aws_region': 'ca-central-1',
    'queue_url': '<test-queue-url>'
}

S3_EVENT_MESSAGE = \
    '{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"ca-central-1","eventTime":' \
    '"2017-11-02T20:46:02.498Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":' \
    '"AWS:AIDAJHTJFFPZ6JYIXFBGK"},"requestParameters":{"sourceIPAddress":"54.200.43.31"},' \
    '"responseElements":{"x-amz-request-id":"9FEAE30337E9E20B","x-amz-id-2":' \
    '"ePCkzMSQgwXGuBUClOGnXto0xgh729x61BY99rS++jkmyjFUVKErNAQzLgBQd6i/VragX4HqoOw="},"s3":' \
    '{"s3SchemaVersion":"1.0","configurationId":"DeliverToTestProcessor","bucket":' \
    '{"name":"ca-central-1-test-bucket","ownerIdentity":{"principalId":"A1YTN9GI8TG7SZ"},' \
    '"arn":"arn:aws:s3:::ca-central-1-test-bucket"},"object":{"key":' \
    '"home/ftp/1/file.txt-1509655557","size":674911,' \
    '"eTag":"939cfcaf3d04deb439927640aa2e0f99","sequencer":"0059FB840A5CF6BEB4"}}}]}'


class TestSQSClient(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')
        self.sqs_client = SQSClient(SQS_CONN_PARAMS, conn_retries=1, msg_wait_seconds=10)

        # Silence some of the errors associated with long lived connections
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    def shortDescription(self):
        return None

        # Test start below

    def test_connection1(self):
        conn = self.sqs_client.client
        assert conn

    def test_message1(self):
        """
        Test sending a message
        """
        response = self.sqs_client.send_message('{"test":"this is a test message"}')
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert 'MessageId' in response

    def test_message2(self):
        """
        Test sending a message
        that resembles a s3 event triggered msg to queue
        """
        response = self.sqs_message = self.sqs_client.send_message(S3_EVENT_MESSAGE)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert 'MessageId' in response

    def test_message3(self):
        """
        Test receiving a message
        """
        messages = self.sqs_client.get_messages(msg_wait_seconds=1)
        assert isinstance(messages, list)

    def test_message4(self):
        """
        Test deleting a message
        """
        self.sqs_message = self.sqs_client.send_message(S3_EVENT_MESSAGE)

        msgs = self.sqs_client.get_messages(num_messages=2)
        for msg in msgs:
            assert self.sqs_client.delete_message(msg)

    def test_message5(self):
        """
        Test deleting a message by handle
        """
        self.sqs_message = self.sqs_client.send_message(S3_EVENT_MESSAGE)

        msgs = self.sqs_client.get_messages(num_messages=1)
        receipt_handle = msgs[0]['ReceiptHandle']
        assert self.sqs_client.delete_message(receipt_handle=receipt_handle)

        # Clean up all messages
        msgs = self.sqs_client.get_messages(msg_wait_seconds=1)
        while msgs:
            msgs = self.sqs_client.get_messages(msg_wait_seconds=1)
            [self.sqs_client.delete_message(msg) for msg in msgs]

    def test_message6(self):
        """
        Test deleting a invalid msg
        """
        with self.assertRaises(KeyError):
            self.sqs_client.delete_message(sqs_message={})

    def test_message7(self):
        """
        Test deleting a invalid msg
        """
        success = self.sqs_client.delete_message(receipt_handle='123abc')  # Fails silent if can't find id
        assert success is True

    def test_message8(self):
        msgs = self.sqs_client.get_messages(num_messages=2)
        assert isinstance(msgs, list)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
