#!/usr/bin/env python
"""Tests cache methods"""
import logging
import unittest
import sys

sys.path.insert(0, '..')
from service_clients.cache.redis_client import RedisClient


CACHE_CONN_PARAMS = {
    'read_host': 'localhost',
    'read_port': 6379,
    'write_host': 'localhost',
    'write_port': 6379
}


# Python2-3 compatibility
if sys.version_info > (3,):
    long = int


class TestCache(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestCache')
        self.client = RedisClient(CACHE_CONN_PARAMS)

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestCache')

    def shortDescription(self):
        return None

    # Test start below

    def test_read_connection(self):
        read_conn = RedisClient(CACHE_CONN_PARAMS).read_connection()
        assert read_conn

    def test_write_connection(self):
        write_conn = RedisClient(CACHE_CONN_PARAMS).write_connection()
        assert write_conn

    def test_read_connection2(self):
        with self.assertRaises(KeyError):
            client = RedisClient(config=None)
            conn = client.read_connection()

    def test_read_connection3(self):
        config = {
            'host': 'localhost',
            'port': 6379
        }
        with self.assertRaises(KeyError):
            RedisClient(config=config).read_connection()

    def test_write_connection2(self):
        with self.assertRaises(KeyError):
            RedisClient(config=None).write_connection()

    def test_write_connection3(self):
        config = {
            'host': 'localhost',
            'port': 6379
        }
        with self.assertRaises(KeyError):
            RedisClient(config=config).write_connection()

    def test_clear_cache_ns(self):
        count = self.client.clear_cache_ns('test:docs')
        assert isinstance(count, (int, long))

    def test_flush_all_cache(self):
        self.client.read_connection()
        assert self.client.flush_cache()

    def test_operations1(self):
        """
        Test set and get
        """
        key = 'test:123:123'

        self.client.write_connection().set(key, 'test data')
        assert self.client.write_connection().get(key)

    def test_operations2(self):
        """
        Test hset and hget
        """
        key = 'test:456:123:123'
        self.client.write_connection().hset(key, 'some_data', '{"hello": "test", "_skip": 1}')
        assert self.client.write_connection().hget(key, 'some_data')


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
