"""
Simple Queue with Redis Backend
Redis list implements a queue
No ack mechanism available here but
could use RPOPLPUSH if needed
https://redis.io/commands/rpoplpush
"""
import logging


from service_clients.cache.redis_client import RedisBaseClient


class RedisQueue(RedisBaseClient):

    def get_queue_msg_count(self, q_name):
        """
        Get queue message count
        Return the size of the queue (list).
        :param q_name: str, redis key (queue name)
        :return:
        """
        try:
            msg_count = self.read_connection().llen(q_name)
        except Exception as e:  # pragma: no cover
            msg_count = 0
            logging.warning("RedisQueue.get_queue_msg_count no data for queue {}. {}".format(q_name, e))
        return msg_count

    def is_empty(self, q_name):
        """
        Return True if the queue is empty, False otherwise.
        :param q_name: str, queue name
        :return: bool, is empty
        """
        return self.get_queue_msg_count(q_name) == 0

    def publish(self, q_name, data):
        """
        Publish msg/item to queue.
        :param q_name: str, queue name
        :param data: str, data (message)
        :return: bool, success
        """
        try:
            self.write_connection().rpush(q_name, data)
        except Exception as e:  # pragma: no cover
            logging.warning("RedisQueue.publish for queue {}, msg {}. {}".format(q_name, data, e))
            return False
        return True

    def flush_queue(self, q_name):
        """
        Flush a queue to clear work for consumer
        :param q_name:
        :return:
        """
        try:
            self.write_connection().delete(q_name)
        except Exception as e:  # pragma: no cover
            logging.exception("RedisQueue.flush_queue {} error {}".format(q_name, e))
            return False
        return True

    def flush_queues(self, q_names):
        """
        Flush all queues
        :return: bool, success
        """
        try:
            self.write_connection().delete(*q_names)
        except Exception as e:  # pragma: no cover
            logging.exception("RedisQueue.flush_queues {} error {}".format(q_names, e))
            return False
        return True

    def get_messages(self, q_name, prefetch_count=100):
        """
        Get messages from queue
        :param q_name: str, queue name
        :param prefetch_count: int, number of msgs to prefetch
            for consumer (default 1000)
        """
        pipe = self.write_connection().pipeline()
        pipe.lrange(q_name, 0, prefetch_count - 1)  # Get msgs (w/o pop)
        pipe.ltrim(q_name, prefetch_count, -1)  # Trim (pop) list to new value

        messages, trim_success = pipe.execute()

        return messages

    def get_message(self, q_name, timeout=None):
        """
        Pop and return an msg/item from the queue.
        If optional args timeout is not None (the default), block
        if necessary until an item is available.
        Allows for blocking via timeout if queue
        does not exist.
        :param q_name: str, queue name
        :param timeout: int, timeout wait seconds (blocking get)
        :return: str, message
        """
        if timeout:
            item = self.read_connection().blpop(q_name, timeout=timeout)
        else:
            item = self.read_connection().lpop(q_name)

        if item:
            item = item[1]
        return item
