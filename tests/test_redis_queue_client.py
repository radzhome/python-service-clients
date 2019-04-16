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
        """
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert isinstance(count, (int, long))

    def test_get_queue_msg_count2(self):
        """
        Test msg count returns 1
        """
        self.client.publish(TEST_QUEUE, '{"test": "data"}')
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert count >= 1

    def test_get_queue_msg_count3(self):
        """
        Test msg count returns 2
        """
        self.client.publish(TEST_QUEUE, '{"test": "data"}')
        self.client.publish(TEST_QUEUE, '{"test": "data"}')
        count = self.client.get_queue_msg_count(TEST_QUEUE)
        assert count >= 2

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

    def test_get_message1(self):
        self.client.flush_queue(TEST_QUEUE)
        message = self.client.get_message(TEST_QUEUE)
        assert not message

    def test_get_message2(self):
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

        message = self.client.get_message(TEST_QUEUE)
        assert message
        assert self.client.get_queue_msg_count(TEST_QUEUE) == 9

        message = self.client.get_message(TEST_QUEUE)
        assert message
        assert self.client.get_queue_msg_count(TEST_QUEUE) == 8

    def test_get_message3(self):
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


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
