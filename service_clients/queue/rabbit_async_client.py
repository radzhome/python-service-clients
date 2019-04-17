from __future__ import unicode_literals
"""
RabbitMQ async clients
"""
import logging

try:
    import pika
except ImportError:
    raise Exception("Optional requires for rabbit (pika) is missing.")


from service_clients.queue.rabbit_client import RabbitQueue
from service_clients.queue.rabbit_client import DIRECT_EXCHANGE


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
