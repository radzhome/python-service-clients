from __future__ import unicode_literals
"""
Redis cache service functions
"""
import logging

from redis import StrictRedis

# Defaults
CONNECT_TIMEOUT_SECS = 5.0  # Control how long to wait while establishing a connection
REQUEST_TIMEOUT_SECS = 120.0  # Request socket timeout


class RedisBaseClient(object):

    def __init__(self, config=None, connect_timeout_secs=CONNECT_TIMEOUT_SECS,
                 request_timeout_secs=REQUEST_TIMEOUT_SECS):
        """
        Load config
        :param config: dict, config
        :param connect_timeout_secs: float, re-connect timeout seconds
        :param request_timeout_secs: float, timeout seconds
        """
        self.read_conn = None
        self.write_conn = None
        self.config = config or {}

        self.CONNECT_TIMEOUT_SECS = connect_timeout_secs
        self.REQUEST_TIMEOUT_SECS = request_timeout_secs

        self.read_connection()

    def _connect(self, host, port):
        return StrictRedis(host=host,
                           port=port,
                           socket_keepalive=False,
                           retry_on_timeout=True,
                           socket_timeout=self.REQUEST_TIMEOUT_SECS,
                           socket_connect_timeout=self.CONNECT_TIMEOUT_SECS)

    def read_connection(self):
        """
        Returns a read connection to redis cache
        """
        if not self.read_conn:
            try:
                self.read_conn = self._connect(self.config['read_host'], self.config['read_port'])
            except KeyError:
                logging.error("RedisCache.read_connection invalid configuration")
                raise
            except Exception as e:
                logging.exception("RedisCache.read_connection unhandled exception {}".format(e))
                raise

        return self.read_conn

    def write_connection(self):
        """
        Returns a write connection to redis cache
        """
        if not self.write_conn:
            try:
                self.write_conn = self._connect(self.config['write_host'], self.config['write_port'])
            except KeyError:
                logging.error("RedisCache.write_connection invalid configuration")
                raise
            except Exception as e:
                logging.exception("RedisCache.write_connection unhandled exception {}".format(e))
                raise

        return self.write_conn


class RedisClient(RedisBaseClient):
    """
    Redis connection cache
    Redis connections are lazy so there really isn't
    anything to manage here i.e. re-connect
    """

    def clear_cache_ns(self, ns, chunk_size=20000):
        """
        Clears a namespace in redis cache.
        This is very time consuming and an expensive hit on cache.
        :param ns: str, namespace i.e your:prefix*
        :param chunk_size: int, chunk size to read cache in
        :return: int, cleared keys
        """
        if not ns.endswith('*'):
            ns += '*'

        count = 0
        cursor = '0'
        keys = []
        while cursor != 0:
            cursor, keys = self.write_connection().scan(cursor=cursor, match=ns, count=chunk_size)
            if keys:
                count += len(keys)
                self.write_conn.delete(*keys)

        del cursor, keys

        return count

    def flush_cache(self):
        """
        Clears the entire cache
        :return: bool, success
        """
        self.write_connection().flushall()
        return True
