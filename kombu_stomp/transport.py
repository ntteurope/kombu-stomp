from __future__ import absolute_import
import contextlib

from kombu.transport import virtual
from kombu import utils
from stomp import exception as exc

from . import stomp


class Message(virtual.Message):
    """Kombu virtual transport message class for kombu-stomp.

    This class extends :py:class:`kombu.transport.virtual.Message`, so it
    keeps STOMP message ID for later use.
    """

    def __init__(self, channel, raw_message):
        # we'll get a message ID only for incoming messages
        if isinstance(raw_message, tuple):
            raw_message, msg_id = raw_message
            self.msg_id = msg_id
        else:
            self.msg_id = None

        super(Message, self).__init__(channel, raw_message)


class QoS(virtual.QoS):
    """Kombu quality of service class for ``kombu-stomp``."""
    def __init__(self, *args, **kwargs):
        self.ids = {}
        super(QoS, self).__init__(*args, **kwargs)

    def append(self, message, delivery_tag):
        self.ids[delivery_tag] = message.msg_id
        super(QoS, self).append(message, delivery_tag)

    def ack(self, delivery_tag):
        self._stomp_ack(delivery_tag)
        return super(QoS, self).ack(delivery_tag)

    def _stomp_ack(self, delivery_tag):
        msg_id = self.ids.pop(delivery_tag, None)
        if msg_id:
            with self.channel.conn_or_acquire() as conn:
                conn.ack(msg_id)


class Channel(virtual.Channel):
    """``kombu-stomp`` channel class."""
    QoS = QoS
    Message = Message

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self._stomp_conn = None

    def _get_many(self, queue, timeout=None):
        """Get next messesage from current active queues."""
        with self.conn_or_acquire() as conn:
            # FIXME(rafaduran): inappropriate intimacy code smell
            return next(conn.message_listener.iterator(timeout=timeout))

    def _put(self, queue, message, **kwargs):
        with self.conn_or_acquire() as conn:
            body = message.pop('body')
            conn.send(self.queue_destination(queue), body, **message)

    def queue_bind(self,
                   queue,
                   exchange=None,
                   routing_key='',
                   arguments=None,
                   **kwargs):
        super(Channel, self).queue_bind(queue,
                                        exchange,
                                        routing_key,
                                        arguments,
                                        **kwargs)
        with self.conn_or_acquire() as conn:
            conn.subscribe(
                self.queue_destination(queue),
                ack='client-individual'
            )

    def queue_unbind(self,
                     queue,
                     exchange=None,
                     routing_key='',
                     arguments=None,
                     **kwargs):
        super(Channel, self).queue_unbind(queue,
                                          exchange,
                                          routing_key,
                                          arguments,
                                          **kwargs)
        with self.conn_or_acquire() as conn:
            conn.unsubscribe(self.queue_destination(queue))

    def queue_destination(self, queue):
        return '/queue/{prefix}{name}'.format(prefix=self.prefix,
                                              name=queue)

    @contextlib.contextmanager
    def conn_or_acquire(self, disconnect=False):
        """Use current connection or create a new one."""
        if not self.stomp_conn.is_connected():
            self.stomp_conn.start()
            self.stomp_conn.connect(**self._get_conn_params())

        yield self.stomp_conn

        if disconnect:
            self.stomp_conn.disconnect()
            self.iterator = None

    @property
    def stomp_conn(self):
        """Property over the stomp.py connection object.

        It will create the connection object at first use.
        """
        if not self._stomp_conn:
            self._stomp_conn = stomp.Connection(self.prefix,
                                                **self._get_params())

        return self._stomp_conn

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @utils.cached_property
    def prefix(self):
        return self.transport_options.get('queue_name_prefix', '')

    def _get_params(self):
        return {
            'host_and_ports': [
                (self.connection.client.hostname or '127.0.0.1',
                 self.connection.client.port or 61613)
            ],
            'reconnect_attempts_max': 1,
        }

    def _get_conn_params(self):
        return {
            'username': self.connection.client.userid,
            'passcode': self.connection.client.password,
            'wait': True,
        }

    def close(self):
        super(Channel, self).close()
        try:
            # TODO (rafaduran): do we need unsubscribe all queues first?
            self.stomp_conn.disconnect()
        except exc.NotConnectedException:
            pass


class Transport(virtual.Transport):
    """Transport class for ``kombu-stomp``."""
    Channel = Channel
