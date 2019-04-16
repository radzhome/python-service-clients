from __future__ import unicode_literals
"""
RabbitMQ queue services
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


class AsyncConsumer(object):
    """
    Uses select connection instead of blocking to consume messages
    quickly without needing to send ack/no ack signals.
    Non-blocking connection allows for concurrency, fire and forget
    Based on -
    https://github.com/pika/pika/blob/master/examples/asynchronous_consumer_example.py
    """

    RECONNECT_SLEEP_SECS = 1  # Sleep time before re-connects
    PREFETCH_COUNT = 2  # Number of messages to pre-fetch for the consumer in single request
    HEARTBEAT = 0

    def __init__(self, queue, routing_key, exchange, message_callback, exchange_type=DIRECT_EXCHANGE,
                 config=None, direct_declare_q_names=None, prefetch_count=PREFETCH_COUNT,
                 reconnect_sleep_secs=RECONNECT_SLEEP_SECS):
        """
        Async consumer from queue to run io loop
        :param queue:
        :param routing_key:
        :param exchange:
        :param exchange_type:
        :param message_callback:
        :param config:
        :param direct_declare_q_names:
        """
        self.connection = None
        self._channel = None
        self._closing = False  # Used to signal shutdown
        self._consumer_tag = None

        self.config = config
        self.msg_callback = message_callback
        self._host = self.config.get('host', 'localhost')
        self._port = self.config.get('port', 5672)
        self.HEARTBEAT = self.config.get('heartbeat', self.HEARTBEAT)

        self._queue_name = queue
        self._routing_key = routing_key
        self._exchange = exchange
        self._exchange_type = exchange_type

        # Allow changing prefect count for consumer
        self.PREFETCH_COUNT = prefetch_count

        # Allow changing reconnection sleep time
        self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs

        if direct_declare_q_names:
            RabbitQueue(self.config).direct_declare_queues(direct_declare_q_names)

    def connect(self):
        """
        This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection
        """
        params = pika.ConnectionParameters(host=self._host, port=self._port, heartbeat=0)

        # TODO: Should we handle IncompatibleProtocolError here? (Only breaks with rabbit 3.7.2 on erlang 20.1.7.1)
        # TODO: See https://gist.github.com/radzhome/b1fa5de488d7f4332a5ceece5a854402
        # return pika.SelectConnection(params, self.on_connection_open, stop_ioloop_on_close=False)
        return pika.SelectConnection(
            parameters=params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def reconnect(self, restart_loop=True):
        """
        Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.
        :param restart_loop: bool, restart the io loop called by run()
        """
        # This is the old connection IOLoop instance, stop its ioloop
        try:
            self.connection.ioloop.stop()
        except AttributeError:
            pass  # run() was not called

        if not self._closing:

            # Create a new connection
            self.connection = self.connect()
            # There is now a new connection, needs a new ioloop to run
            if restart_loop:  # pragma: no cover
                self.connection.ioloop.start()

        return True

    def on_channel_open(self, channel):  # pragma: no cover
        """
        This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object
        """
        self._channel = channel
        logging.debug("AsyncConsumer.on_channel_open Channel opened, adding channel close callback")

        self._channel.add_on_close_callback(self.on_channel_closed)

        # self.setup_exchange()
        self.start_consuming()

    def on_connection_open(self, unused_connection):  # pragma: no cover
        logging.debug("AsyncConsumer.on_connection_open Adding connection close callback")
        self.connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        logging.error('AsyncConsumer.on_connection_open_error Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, connection, reply_code, reply_text):  # pragma: no cover
        """
        Added as callback 'add_on_close_callback' in  on_connection_open
        Invoked when the connection to RabbitMQ is
        closed either due to shutdown or unexpectedly.
        If unexpected, we will reconnect to RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given
        """
        self._channel = None
        if self._closing:
            # Shutdown invoked
            self.connection.ioloop.stop()
        else:
            # Connection closed unexpectedly
            logging.warning("AsyncConsumer.on_connection_closed closed with reply code '{}', reply text '{}', "
                            "re-opening connection".format(reply_code, reply_text))
            self.connection.add_timeout(self.RECONNECT_SLEEP_SECS, self.reconnect)

    def on_channel_closed(self, channel, reply_code, reply_text):  # pragma: no cover
        """
        Added as callback 'add_on_close_callback' in on_channel_open
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed
        """
        logging.warning(
            "AsyncConsumer.on_channel_closed Channel {} was closed: ({}) {}".format(channel, reply_code, reply_text))
        self.connection.close()

    def open_channel(self):  # pragma: no cover
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        logging.debug("AsyncConsumer.open_channel Creating a new channel")
        self.connection.channel(on_open_callback=self.on_channel_open)

    def start_consuming(self):  # pragma: no cover
        """
        This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        logging.debug("AsyncConsumer.start_consuming Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._channel.basic_qos(prefetch_count=self.PREFETCH_COUNT)
        self._consumer_tag = self._channel.basic_consume(self.on_message, self._queue_name, auto_ack=True)

    def stop_consuming(self):  # pragma: no cover
        """
        Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            logging.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_consumer_cancelled(self, method_frame):  # pragma: no cover
        """
        Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logging.debug(
            "AsyncConsumer.on_consumer_cancelled Consumer was cancelled, shutting down: {}".format(method_frame))
        if self._channel:
            self._channel.close()

    def on_cancelok(self, unused_frame):  # pragma: no cover
        """
        This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame
        """
        logging.debug(
            "AsyncConsumer.on_cancelok RabbitMQ acknowledged the cancellation of the consumer, closing the channel")
        self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):  # pragma: no cover
        """
        Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param unused_channel: pika.channel.Channel, unused_channel The channel object
        :param basic_deliver: pika.Spec.Basic.Deliver, basic_deliver method
        :param properties: pika.Spec.BasicProperties, properties
        :param str|unicode body: The message body
        """
        # logging.debug("AsyncConsumer.on_message Received message # {} from {}"
        #               "".format(basic_deliver.delivery_tag, properties.app_id))
        self.msg_callback(body)

    def run(self):  # pragma: no cover
        """
        Run the consumer loop by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self.connection = self.connect()
        self.connection.ioloop.start()

    def shutdown(self, signum, stack):  # pragma: no cover
        """
        Stops all the things
        :param signum: int, signal num
        :param stack: stack object
        """
        logging.info('AsyncConsumer.shutdown Stopping')
        self._closing = True
        self.stop_consuming()
        self.connection.ioloop.start()
        logging.info('AsyncConsumer.shutdown Stopped')
