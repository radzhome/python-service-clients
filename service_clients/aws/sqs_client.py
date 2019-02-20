from __future__ import unicode_literals
"""
SQS feed queue core class


Sample SQS message from S3 file created trigger:

{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-west-2",
      "eventTime": "2009-03-21T03:24:48.558Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:someid"
      },
      "requestParameters": {
        "sourceIPAddress": "1.2.3.4"
      },
      "responseElements": {
        "x-amz-request-id": "some-request-id",
        "x-amz-id-2": "some-id"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "triggerName",
        "bucket": {
          "name": "us-west-2-feeds-uat",
          "ownerIdentity": {
            "principalId": "some-id"
          },
          "arn": "arn:aws:s3:::us-west-2-bucket"
        },
        "object": {
          "key": "some file",
          "size": 1319506,
          "eTag": "some-tag",
          "sequencer": "some-sequence"
        }
      }
    }
  ]
}

"""
import logging
import time

import boto3

# Try to get ujson if available
try:
    import ujson as json
except ImportError:
    import json


class SQSClient:  # pragma: nocover
    """
    SQS class encapsulates queue operations,
    This is not covered in unit test test coverage,
    but in integration tests since its an external process
    """

    # Request timeout to poll for msg, must be 0 to 20, poll seconds
    MSG_WAIT_SECONDS = 20
    # Make message invisible to other consumers. Defaults via SQS to 30, visibility timeout
    MSG_INVISIBLE_SECONDS = 14

    RECONNECT_SLEEP_SECS = 0.5
    CONN_RETRIES = 20

    def __init__(self, config, msg_wait_seconds=MSG_WAIT_SECONDS,
                 msg_invisible_seconds=MSG_INVISIBLE_SECONDS, reconnect_sleep_secs=RECONNECT_SLEEP_SECS,
                 conn_retries=CONN_RETRIES):
        """
        Load config from passed params or override with defaults
        :param config: dict with access_key_id, secret_access_key, bucket name
        :return: None
        """
        # Load from passed params or override with defaults
        try:
            self.config = config
            self.access_key_id = self.config['access_key_id']
            self.secret_access_key = self.config['secret_access_key']
            self.aws_region = self.config['aws_region']
            self.queue_url = self.config['queue_url']

            self.MSG_WAIT_SECONDS = msg_wait_seconds
            self.MSG_INVISIBLE_SECONDS = msg_invisible_seconds
            self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs
            self.CONN_RETRIES = conn_retries
        except Exception as e:
            logging.exception("SQSClient.__init__ configuration error {}".format(e))
            self.access_key_id = None
            self.secret_access_key = None
            self.aws_region = None
            self.queue_url = None
            self.config = None

        self.connection_attempt = 0
        self.client = None
        self.connect()

    def connect(self):
        """
        Establish SQS connection
        """
        try:
            self.client = boto3.client('sqs',
                                       region_name=self.aws_region,
                                       aws_access_key_id=self.access_key_id,
                                       aws_secret_access_key=self.secret_access_key)
            self.connection_attempt = 0  # Got queue connection, reset retries
        except Exception as e:
            logging.exception("SQSClient.connect failed with params {}, error {}".format(self.config, e))
            self.connection_attempt += 1
            if self.connection_attempt >= self.CONN_RETRIES:
                raise

    def get_messages(self, num_messages=1, msg_invisible_seconds=None, msg_wait_seconds=None):
        """
        Get messages from sqs feed queue
        :param num_messages: int, number of messages to get, max is 10
        :param msg_wait_seconds: int, time to wait (poll time between retries)
        :param msg_invisible_seconds: int, how long the message is invisible to other consumers
        :return: list, of sqs messages object
        """
        if msg_invisible_seconds is None:
            msg_invisible_seconds = self.MSG_INVISIBLE_SECONDS

        if msg_wait_seconds is None:
            msg_wait_seconds = self.MSG_WAIT_SECONDS

        try:
            # Long polling for a message from SQS (list of 1 message)
            response = self.client.receive_message(MaxNumberOfMessages=num_messages,
                                                   QueueUrl=self.queue_url,
                                                   WaitTimeSeconds=msg_wait_seconds,
                                                   VisibilityTimeout=msg_invisible_seconds) or {}
            sqs_messages = response.get('Messages') or []

        except Exception as e:
            # I.e. gaierror: [Errno -2] Name or service not known
            logging.exception("SQSClient.get_messages error, retrying. {}".format(e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            if self.connection_attempt >= self.CONN_RETRIES:
                raise
            sqs_messages = self.get_messages()

        return sqs_messages

    def delete_message(self, sqs_message=None, receipt_handle=None):
        """
        Delete an sqs msg
        :param sqs_message: dict, message with receipt handle (optional)
        :param receipt_handle: str, receipt handle associated with
            the message to delete (optional)
        :return:
        """
        try:
            receipt_handle = receipt_handle or sqs_message['ReceiptHandle']
        except KeyError:
            logging.error("SQSClient.delete_message missing 'ReceiptHandle' key in message, required for delete")
            raise

        try:
            self.client.delete_message(QueueUrl=self.queue_url,
                                       ReceiptHandle=receipt_handle)
        except self.client.exceptions.ReceiptHandleIsInvalid:
            # Message was already deleted, handle no longer valid, old msg
            pass
        except Exception as e:
            logging.exception("SQSClient.delete_message error, retrying. {}".format(e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            if self.connection_attempt >= self.CONN_RETRIES:
                raise
            self.delete_message(sqs_message)

        return True

    def send_message(self, body, delay_seconds=0):
        """
        For testing only, send a message
        :param body: str, message_content
        :param delay_seconds: int, time to make message visible
        :return: bool, success
        """
        if isinstance(body, dict):
            body = json.dumps(body)

        return self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=body,
            DelaySeconds=delay_seconds,
        )
