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

    def publish_multiple(self, q_name, data_list):
        """
        Publish multiple msg/items to queue.
        :param q_name: str, queue name
        :param data_list: list of str, data (message)
        :return: bool, success
        """
        try:
            # pipe = self.write_connection().pipeline()
            # for data in data_list:
            #     # pipe.rpush(q_name, data)  # TODO: * data_list?
            #     pipe.rpush(q_name, data)  # TODO: * data_list?
            # pipe.execute()
            self.write_connection().rpush(q_name, *data_list)
        except Exception as e:  # pragma: no cover
            logging.warning("RedisQueue.publish_multiple for queue {}. {}".format(q_name, e))
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
        if timeout is not None:
            msg = self.read_connection().blpop(q_name, timeout=timeout)
            if msg:
                msg = msg[1]
        else:
            msg = self.read_connection().lpop(q_name)  # TODO: Returns single item? or tuple?

        return msg

    def get_message_safe(self, q_name, timeout=0, processing_prefix='processing'):
        """
        Retrieve a message but also send it to
        a processing queue for later acking
        :param q_name: str, queue name
        :param timeout:
        :param processing_prefix:
        :return:
        """
        # Too bad blpoplpush does not exist
        # item = self.read_connection().brpoplpush(q_name, "{}:{}".format(q_name, processing_prefix), timeout=timeout)

        msg = self.get_message(q_name=q_name, timeout=timeout)
        if msg:
            self.write_connection().lpush("{}:{}".format(q_name, processing_prefix), msg)
        return msg

    def ack_message_safe(self, q_name, message, processing_prefix='processing'):
        """
        Acknowledge a message has been processed
        :param q_name: str, queue name
        :param message: str, message value
        :param processing_prefix: str, prefix of processing queue name
        :return: bool, success
        """
        self.read_connection().lrem("{}:{}".format(q_name, processing_prefix), -1, message)
        return True

    def requeue_message_safe(self, q_name, processing_prefix='processing'):
        """
        Move unprocessed messages from processing queue
        to original queue for re-processing
        :param q_name:
        :param processing_prefix:
        :return: bool, success
        """
        msgs = self.write_connection().lrange("{}:{}".format(q_name, processing_prefix), 0, -1)  # Get all msgs
        if msgs:
            msgs = msgs[::-1]  # Reverse order
            pipe = self.write_connection().pipeline()
            pipe.rpush(q_name, *msgs)
            pipe.ltrim("{}:{}".format(q_name, processing_prefix), 0, -1)  # Cleanup
            pipe.execute()
        return True
