from __future__ import unicode_literals
"""
RabbitMQ standard blocking queue client
"""
import logging
import time

try:
    import pika
except ImportError:
    raise Exception("Optional requires for rabbit (pika) is missing.")

DIRECT_EXCHANGE = 'direct'


class RabbitQueueConnExceeded(Exception):
    pass


class RabbitQueue(object):
    """
    Uses blocking connection to publish messages, declare queues,
    flush queues, get status, info, and 1 shot conume (get messges)
    Get messages function for basic get (consume) used by cron jobs
    to consume a queue fully
    """

    # Some shared queue settings, only publisher defines queues
    AUTO_DELETE = False
    DURABLE = True  # Durable = survives restart
    DURABLE_EXCHANGE = True

    RECONNECT_SLEEP_SECS = 2

    # How many times we try to connect (publish) before failing
    RETRY_ATTEMPTS = 20

    # How often to send heartbeats to rabbit
    HEARTBEAT = 60

    # Default connection timeout is
    CONNECT_TIMEOUT = 10  # In seconds.

    def __init__(self, config=None, retries=None, reconnect_sleep_secs=None, timeout=None):
        """
        Loads the connection, queue, exchange, routing key configuration
        :param config: dict, config params
        :param retries: int, number of publish retries
        :param reconnect_sleep_secs: int, number of seconds to wait between re-connect
        """
        self.connection = None
        self._channel = None

        self.config = config

        # Defaults
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 5672)
        self.RETRY_ATTEMPTS = retries or self.RETRY_ATTEMPTS
        self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs or self.RECONNECT_SLEEP_SECS
        self.CONNECT_TIMEOUT = timeout or self.CONNECT_TIMEOUT
        # heartbeat - None to use servers value, 0 to disable, or Max b/w this value and servers
        # heartbeat=0 means Do not send heartbeats. Old default 580, now 60.
        # Socket error when workers idle for long period of time and heartbeat exceeded
        # Disabling heartbeats might improve performance in situations with a great number of connections,
        # but might lead to connections dropping in the presence of network devices that close inactive connections.
        # https://github.com/pika/pika/issues/266
        self.HEARTBEAT = self.config.get('heartbeat', self.HEARTBEAT)

        self.connection_params = pika.ConnectionParameters(host=self.host, port=self.port, heartbeat=self.HEARTBEAT,
                                                           blocked_connection_timeout=self.CONNECT_TIMEOUT,
                                                           connection_attempts=1)
        self.connect(log_error=True)

    def connect(self, log_error=False):
        """
        Create a connection to rabbit
        :param log_error: log connection error
        :return: connection
        """
        connected = False
        retries = 0
        while not connected:
            try:
                self.connection = pika.BlockingConnection(self.connection_params)
                # Force a new channel here
                self._channel = self.connection.channel()
                connected = True
            except Exception as e:  # pragma: no cover
                # pika.exceptions.ConnectionClosed:
                # I.e. Connection reset by peer
                # I.e. Connection to <host>:5672 failed: timeout
                # I.e. Connection refused (Maybe rabbit is out of space or mem) cd5dx
                if log_error:
                    logging.error("RabbitQueue.connect error on init {}, retrying".format(e))
                else:
                    logging.warning("RabbitQueue.connect error on re-connect {}, retrying".format(e))

                retries += 1
                if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                    self._close_connection()
                    logging.exception("RabbitQueue.connect retires exceeded {}".format(e))
                    raise RabbitQueueConnExceeded("RabbitQueue.connect retires exceeded")

                time.sleep(self.RECONNECT_SLEEP_SECS)

    def _get_channel(self):
        """
        Returns currently open channel or establishes a
        new blocking connection channel.
        :return: channel
        """
        # These is_open checks don't always seem to be telling the truth
        if self.connection and self._channel and self.connection.is_open and self._channel.is_open:
            return self._channel
        else:  # pragma: no cover
            logging.debug("RabbitQueue._get_channel getting new blocking channel connection")

            self.connect()
            return self.connection.channel()

    def publish(self, exchange, data, expiration=None, retries=0):  # pragma: no cover
        """
        Does a basic publish with exchange equal to routing key
        :param exchange: str, name of exchange and queue
        :param retries: int, number of retries
        :param data: dict, msg
        :param expiration: int, expiration of the message in milliseconds
        :return:
        """
        # Going non-persistent
        props = pika.BasicProperties(content_type='application/json', delivery_mode=1)

        # Optional message expiration i.e. for mapping
        if expiration:
            props.expiration = str(expiration)  # pika wants a str
            props.timestamp = int(time.time())

        result = True
        try:
            self._channel.basic_publish(exchange=exchange, routing_key=exchange, body=data, properties=props)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosed, AttributeError,
                pika.exceptions.ConnectionClosed) as e:  # pragma: no cover
            # TODO: Try FileNotFoundError exception
            # If exceptions.ConnectionClosed, get channel should re-open so should not see this error
            logging.warning("RabbitQueue.publish connection broken {}, re-establishing connection".format(e))
            self.connect()  # Reset connection
            result = self.publish(exchange, data, expiration, retries + 1)
        except Exception as e:  # pragma: no cover
            logging.exception("RabbitQueue.publish unhandled error {}".format(e))
            raise  # Will raise instead of result False after retry attempts exceeded

        return result

    def direct_declare(self, exchange, auto_delete=AUTO_DELETE, durable=DURABLE,
                       durable_exchange=DURABLE_EXCHANGE):  # pragma: no cover
        """
        Declare exchange and queue using direct style
        Call direct declare only when workers start
        Declares the exchange, the queue and binds
        the exchange directly to 1 queue with same name
        This will create a queue bound to an exchange of the same name with
        a routing key of the same name.  exchange type will be direct
        :param exchange: str, name of exchange, queue and routing key to bind on
            get info on it if it does
        :param auto_delete: bool, auto delete queue, exchange on service reboot
        :param durable: bool, make queues durable
        :param durable_exchange: bool, make exchanges durable
        """
        logging.debug("RabbitQueue.direct_declare declaring queues, exchanges & bindings")
        channel = self._get_channel()

        try:
            channel.exchange_declare(exchange=exchange,
                                     exchange_type=DIRECT_EXCHANGE,
                                     durable=durable_exchange,
                                     auto_delete=auto_delete)
        except pika.exceptions.ChannelClosed:
            # If any declaration changes, have to delete the queue/exchange first
            channel = self._get_channel()
            channel.exchange_delete(exchange=exchange)
            channel.exchange_declare(exchange=exchange,
                                     exchange_type=DIRECT_EXCHANGE,
                                     durable=durable_exchange,
                                     auto_delete=auto_delete)
        except Exception as e:  # pragma: no cover
            logging.exception("RabbitQueue.direct_declare exchange unhandled error {}".format(e))
            raise

        try:
            channel.queue_declare(queue=exchange, durable=durable, auto_delete=auto_delete)
        except pika.exceptions.ChannelClosed:  # pragma: no cover
            # If any declaration changes, have to delete the queue/exchange first
            channel = self._get_channel()
            channel.queue_delete(queue=exchange)
            channel.queue_declare(queue=exchange, durable=durable, auto_delete=auto_delete)
        except Exception as e:  # pragma: no cover
            logging.exception("RabbitQueue.direct_declare queue unhandled error {}".format(e))
            raise

        channel.queue_bind(exchange=exchange, queue=exchange, routing_key=exchange)
        return True

    def direct_declare_queues(self, q_names):
        """
        Declares all queues for pipeline
        :param q_names: list, of queue names
        :return: bool, success
        """
        for q_name in q_names:
            self.direct_declare(q_name)
        return True

    def flush_queue(self, q_name):
        """
        Flush a queue to clear work for consumer
        :param q_name:
        :return:
        """
        try:
            self._get_channel().queue_purge(q_name)
        except Exception as e:  # pragma: no cover
            logging.exception("RabbitQueue.flush_queue {} error {}".format(q_name, e))
            return False
        return True

    def flush_queues(self, q_names):
        """
        Flush all queues
        :return: bool, success
        """
        for q_name in q_names:
            self.flush_queue(q_name)
        return True

    def get_queue_info(self, q_name):
        """
        Gets queue information
        :param q_name: str, name of the queue
        :return: response object, count in res.method.message_count or None if not found
        """
        try:
            info = self._get_channel().queue_declare(queue=q_name, passive=True)
        except pika.exceptions.ChannelClosed:
            info = None
        return info

    def get_queue_msg_count(self, q_name):
        """
        Get queue message count
        :param q_name: str, name of the queue
        :return: int, size of queue
        """
        try:
            res = self.get_queue_info(q_name)
            msg_count = res.method.message_count
        except Exception as e:  # pragma: no cover
            msg_count = 0
            logging.warning("RabbitQueue.get_queue_msg_count no data for queue {}. {}".format(q_name, e))
        return msg_count

    def get_messages(self, q_name, prefetch_count=1000):
        """
        Get messages from queue
        :param q_name: str, queue name
        :param prefetch_count: int, number of msgs to
            prefetch for consumer (default 1000)
        """
        logging.debug("RabbitQueue.get_messages for queue '{}'".format(q_name))
        self._channel.basic_qos(prefetch_count=prefetch_count)

        messages = []

        while self._channel.queue_declare(q_name, passive=True).method.message_count > 0:
            # Get a message, no ack required
            status, properties, message = self._channel.basic_get(q_name, auto_ack=True)
            messages.append(message)

        logging.debug("RabbitQueue.get_message queue '{}' is drained".format(q_name))
        return messages

    def _close_connection(self):
        """
        Close db connections
        :return: None
        """
        if self.connection:
            try:
                self.connection.close()
            except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed,
                    pika.exceptions.ConnectionClosed):  # pragma: no cover
                pass

    def __del__(self):
        """
        Close conns when object is destroyed
        Channel will be closed once conn closed
        """
        self._close_connection()
