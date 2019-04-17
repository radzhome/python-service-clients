#!/usr/bin/env python
"""Tests redis queue methods"""
import logging
import unittest
import sys
import time
sys.path.insert(0, '..')
from service_clients.queue.redis_client import RedisQueue


CACHE_CONN_PARAMS = {
    'read_host': 'localhost',
    'read_port': 6379,
    'write_host': 'localhost',
    'write_port': 6379
}

TEST_QUEUE = 'queue:test:msgs'
TEST_QUEUE2 = 'queue:test:docs'

# Python2-3 compatibility
if sys.version_info > (3,):
    long = int


class TestRedisQueue(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestCache')
        self.client = RedisQueue(CACHE_CONN_PARAMS)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestCache')

    def shortDescription(self):
        return None

    # Test start below

    def test_get_queue_msg_count1(self):
        """
        Test msg count returns int
        Non existent list
        """
        self.client.flush_queue(TEST_QUEUE)
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert isinstance(count, (int, long))
        assert not count

    def test_get_queue_msg_count2(self):
        """
        Test msg count returns
        valid count
        """
        self.client.flush_queue(TEST_QUEUE)

        for i in range(2):
            self.client.publish(TEST_QUEUE, '{"test": "data"}')
            count = self.client.get_queue_msg_count(TEST_QUEUE)
            assert count == i + 1

    def test_get_queue_msg_count3(self):
        """
        Test msg count, empty list
        """
        self.client.flush_queue(TEST_QUEUE)
        self.client.publish(TEST_QUEUE, '{"test": "data"}')
        self.client.get_messages(TEST_QUEUE)  # empty list
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert isinstance(count, (int, long))
        assert not count

    def test_flush1(self):
        assert self.client.flush_queue(TEST_QUEUE)
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert not count

    def test_flush2(self):
        self.client.publish(TEST_QUEUE, '{"test": "data"}')
        self.client.publish(TEST_QUEUE2, '{"test": "data"}')
        assert self.client.flush_queues(q_names=[TEST_QUEUE, TEST_QUEUE2])
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert not count
        assert not self.client.get_queue_msg_count(TEST_QUEUE2)

    def test_get_messages1(self):
        self.client.flush_queue(TEST_QUEUE)
        messages = self.client.get_messages(TEST_QUEUE)
        assert not messages

    def test_get_messages2(self):
        self.client.flush_queue(TEST_QUEUE)
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        self.client.publish(TEST_QUEUE, '{"test": "data1"}')
        self.client.publish(TEST_QUEUE, '{"test": "data2"}')
        self.client.publish(TEST_QUEUE, '{"test": "data3"}')
        self.client.publish(TEST_QUEUE, '{"test": "data4"}')
        self.client.publish(TEST_QUEUE, '{"test": "data5"}')
        self.client.publish(TEST_QUEUE, '{"test": "data6"}')
        self.client.publish(TEST_QUEUE, '{"test": "data7"}')
        self.client.publish(TEST_QUEUE, '{"test": "data8"}')
        self.client.publish(TEST_QUEUE, '{"test": "data9"}')

        messages = self.client.get_messages(TEST_QUEUE, prefetch_count=5)
        assert len(messages) == 5, len(messages)

        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert count == 5, count

        messages = self.client.get_messages(TEST_QUEUE, prefetch_count=5)
        assert len(messages) == 5, len(messages)

        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert count == 0, count

    def test_get_message1(self):
        self.client.flush_queue(TEST_QUEUE)
        message = self.client.get_message(TEST_QUEUE)
        assert not message

    def test_get_message2(self):
        """
        Test simple message get
        """
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        message = self.client.get_message(TEST_QUEUE)
        assert message

    def test_get_message3(self):
        """
        Test order
        """
        self.client.flush_queue(TEST_QUEUE)
        num_msgs = 10
        for i in range(num_msgs):
            self.client.publish(TEST_QUEUE, '{{"test": "data{}"}}'.format(i))

        for i in range(num_msgs):
            message = self.client.get_message(TEST_QUEUE)
            assert message
            assert message == bytes('{{"test": "data{}"}}'.format(i).encode('utf-8')), message
            assert self.client.get_queue_msg_count(TEST_QUEUE) == num_msgs - i - 1

    def test_get_message4(self):
        """
        If the key (queue) does not exist
        the get message function will block (wait) for timeout
        seconds for a msg
        """
        self.client.flush_queue(TEST_QUEUE)
        s = time.time()
        message = self.client.get_message(TEST_QUEUE, timeout=1)
        assert time.time() - s >= 1  # Should at least be 1 sec wait due to timeout since queue is flushed (deleted)
        assert not message

    def test_get_message5(self):
        """
        Test blocking get msg with
        actual message
        """
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        message = self.client.get_message(TEST_QUEUE, timeout=10)
        assert message
        assert isinstance(message, bytes)

    def test_message_safe1(self):
        """
        Get message using safe
        Count processing queue (list)
        """
        processing_prefix = 'processing'
        test_queue_processing = "{}:{}".format(TEST_QUEUE, processing_prefix)
        self.client.flush_queue(TEST_QUEUE)
        self.client.flush_queue(test_queue_processing)
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        self.client.publish(TEST_QUEUE, '{"test": "data1"}')
        self.client.publish(TEST_QUEUE, '{"test": "data2"}')
        self.client.get_message_safe(TEST_QUEUE, timeout=0, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=0, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)

        processing_count = self.client.get_queue_msg_count(test_queue_processing)
        assert processing_count == 4

    def test_message_safe2(self):
        """
        Ack a message
        """
        processing_prefix = 'processing'
        test_queue_processing = "{}:{}".format(TEST_QUEUE, processing_prefix)
        self.client.flush_queue(TEST_QUEUE)
        self.client.flush_queue(test_queue_processing)
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        self.client.publish(TEST_QUEUE, '{"test": "data0"}')
        self.client.publish(TEST_QUEUE, '{"test": "data1"}')
        self.client.publish(TEST_QUEUE, '{"test": "data2"}')
        message = self.client.get_message_safe(TEST_QUEUE, timeout=0, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=0, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)
        self.client.get_message_safe(TEST_QUEUE, timeout=None, processing_prefix=processing_prefix)

        self.client.ack_message_safe(TEST_QUEUE, message)
        processing_count = self.client.get_queue_msg_count(test_queue_processing)
        assert processing_count == 3

        self.client.ack_message_safe(TEST_QUEUE, message)
        processing_count = self.client.get_queue_msg_count(test_queue_processing)
        assert processing_count == 2

        self.client.ack_message_safe(TEST_QUEUE, message)
        processing_count = self.client.get_queue_msg_count(test_queue_processing)
        assert processing_count == 2  # Still 2, b/c data0 msg not found anymore

        self.client.ack_message_safe(TEST_QUEUE, '{"test": "data2"}')
        processing_count = self.client.get_queue_msg_count(test_queue_processing)
        assert processing_count == 1

    def test_message_safe3(self):
        """
        Requeue unacked messages
        """
        processing_prefix = 'processing'
        test_queue_processing = "{}:{}".format(TEST_QUEUE, processing_prefix)
        self.client.flush_queue(TEST_QUEUE)
        self.client.flush_queue(test_queue_processing)

        num_msgs = 10
        for i in range(num_msgs):
            self.client.publish(TEST_QUEUE, '{{"test": "data{}"}}'.format(i))
            message = self.client.get_message_safe(TEST_QUEUE, timeout=0, processing_prefix=processing_prefix)
            assert message

        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert not count, count

        self.client.requeue_message_safe(TEST_QUEUE, processing_prefix)
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert count == num_msgs

        # Ensure order is right
        for i in range(num_msgs):
            message = self.client.get_message(TEST_QUEUE)
            assert message == bytes('{{"test": "data{}"}}'.format(i).encode('utf-8')), message


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
