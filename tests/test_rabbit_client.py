#!/usr/bin/env python
"""Tests queue methods"""
import logging
import sys
import unittest

sys.path.insert(0, '..')
from service_clients.queue.rabbit_client import RabbitQueue
from service_clients.queue.rabbit_async_client import AsyncConsumer

TEST_QUEUE = 'test.msg_queue.test'

QUEUE_CONN_PARAMS = {
    'host': 'localhost',
    'port': 5672
}


# Test callback function
def process_msg(body):
    logging.debug("got msg {}".format(body))
    return True


# Test callback function for get_messages
def process_msg2(ch, method, properties, body):
    logging.debug("got msg {}".format(body))
    return True


class TestRabbitQueue(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    @classmethod
    def setUpClass(cls):
        """setup_class() before any methods in this class, init class"""
        cls.queue = RabbitQueue(QUEUE_CONN_PARAMS)

    def shortDescription(self):
        return None

    # Test start below

    def test_configuration1(self):
        """
        Config is not passed
        """
        with self.assertRaises((TypeError, AttributeError)):
            queue = RabbitQueue()
            queue.connect()

    def test_publish1(self):
        """
        Publish without declare should be passive, str
        """
        publish = self.queue.publish(TEST_QUEUE, 'this is a test msg')
        assert publish

    def test_publish2(self):
        """
        Publish json
        """
        self.queue.direct_declare(TEST_QUEUE)

        publish = self.queue.publish(TEST_QUEUE, '{"test": "this is a test msg"}')
        assert publish

    def test_publish3(self):
        """
        Publish dict, unhashable type TypeError
        """
        self.queue.direct_declare(TEST_QUEUE)

        with self.assertRaises(TypeError):
            self.queue.publish(TEST_QUEUE, {'test': "this is a test msg"})

    def test_get_messages1(self):
        """
        This time declare the queue, publish and check consume
        """
        self.queue.direct_declare(TEST_QUEUE)
        self.queue.publish(TEST_QUEUE, 'this is a test msg')

        messages = self.queue.get_messages(TEST_QUEUE, prefetch_count=1)
        assert len(messages) >= 1

    def test_direct_declare1(self):
        assert self.queue.direct_declare(TEST_QUEUE)

    def test_flush_queue1(self):
        self.queue.direct_declare(TEST_QUEUE)
        assert self.queue.flush_queue(TEST_QUEUE)

    def test_get_queue_msg_count1(self):
        """
        Publish a msg, check queue has at least 1 msg count
        """
        self.queue.direct_declare(TEST_QUEUE)
        self.queue.publish(TEST_QUEUE, 'this is a test msg')

        msg_count = self.queue.get_queue_msg_count(TEST_QUEUE)
        assert isinstance(msg_count, int)

    def test_get_queue_info1(self):
        """
        Queue info contains:
        {'frame_type': 1, 'channel_number': 1, 'method':
        <Queue.DeclareOk(['consumer_count=0', 'message_count=0', 'test.tickets.test'])>}
        """
        self.queue.direct_declare(TEST_QUEUE)
        assert TEST_QUEUE in str(self.queue.get_queue_info(TEST_QUEUE).__dict__)

    def test_get_queue_info2(self):
        """
        Non-existent queue
        """
        assert not self.queue.get_queue_info('non-existent-rabbit-queue')

    def test_declare_queues1(self):
        assert self.queue.direct_declare_queues([TEST_QUEUE, ])

    def test_flush_queues1(self):
        assert self.queue.flush_queues(TEST_QUEUE, )


class TestAsyncConsumer(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up')

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down')

    def shortDescription(self):
        return None

    # Test start below

    def test_configuration1(self):
        """
        Config is not passed
        """
        with self.assertRaises(AttributeError):
            AsyncConsumer(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE, 'direct', process_msg)

    def test_configuration2(self):
        """
        With direct declare
        """
        consumer = AsyncConsumer(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE, 'direct', process_msg,
                                 config=QUEUE_CONN_PARAMS, direct_declare_q_names=[TEST_QUEUE, ])
        assert consumer

    def test_reconnect1(self):
        consumer = AsyncConsumer(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE, 'direct', process_msg,
                                 config=QUEUE_CONN_PARAMS)
        assert consumer.reconnect(restart_loop=False)  # Will start run() loop if True

    def test_consumer1(self):
        consumer = AsyncConsumer(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE, 'direct', process_msg,
                                 config=QUEUE_CONN_PARAMS)
        assert consumer


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
