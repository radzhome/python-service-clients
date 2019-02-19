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

# TODO - add all tests

#
# client = SQSClient(conf, conn_retries=1, msg_wait_seconds=10)
#
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
# # Clean up all messages
# while msgs:
#     [client.delete_message(msg) for msg in msgs]
#     msgs = client.get_messages(msg_wait_seconds=1)


class TestSQSClient(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')
        self.sqs_client = SQSClient(SQS_CONN_PARAMS)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    def shortDescription(self):
        return None

        # Test start below

    def test_connection1(self):
        conn = self.sqs_client.client
        assert conn
