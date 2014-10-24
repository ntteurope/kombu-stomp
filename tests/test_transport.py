from stomp import exception as exc

from kombu_stomp import transport
from kombu_stomp.utils import mock
from kombu_stomp.utils import unittest


class MessageTests(unittest.TestCase):
    def setUp(self):
        self.raw_message = {
            'content-encoding': 'utf-8',
            'content-type': 'application/json',
            'properties': {
                'body_encoding': u'base64',
                u'delivery_info': {
                    u'priority': 0,
                    'routing_key': u'simple_queue',
                    'exchange': u'simple_queue'
                },
                'delivery_mode': 2,
                'delivery_tag': '423e3830-e67a-458d-9aa0-f58df4d01639'
            },
            'body': 'eyJoZWxsbyI6ICJ3b3JsZCJ9'
        }
        self.channel = mock.Mock(**{
            'decode_body.return_value': self.raw_message['body'],
        })
        self.msg_id = 'msg-id'

    def test_init__raw_message_only(self):
        message = transport.Message(self.channel, self.raw_message)
        # The encode is required in Python 3, since kombu is doing it
        self.assertEqual(self.raw_message['body'].encode(), message.body)
        self.assertIsNone(message.msg_id)

    def test_init__raw_message_and_id(self):
        message = transport.Message(
            self.channel,
            (self.raw_message, self.msg_id),
        )
        # The encode is required in Python 3, since kombu is doing it
        self.assertEqual(self.raw_message['body'].encode(), message.body)
        self.assertEqual(message.msg_id, self.msg_id)


class QoSTests(unittest.TestCase):
    def setUp(self):
        self.channel = mock.MagicMock()
        self.qos = transport.QoS(self.channel)
        self.msg_id = 'msg-id'
        self.msg = mock.Mock(msg_id=self.msg_id)
        self.delivery_tag = '423e3830-e67a-458d-9aa0-f58df4d01639'

    @mock.patch('kombu.transport.virtual.QoS.append')
    def test_append__calls_super(self, append):
        self.qos.append(self.msg, self.delivery_tag)
        append.assert_called_once_with(self.msg, self.delivery_tag)

    def test_append__saves_message_id_reference(self):
        self.qos.append(self.msg, self.delivery_tag)
        self.assertDictEqual(self.qos.ids, {self.delivery_tag: self.msg_id})

    @mock.patch('kombu.transport.virtual.QoS.ack')
    def test_ack__calls_super(self, ack):
        self.qos.ack(self.delivery_tag)
        ack.assert_called_once_with(self.delivery_tag)

    @mock.patch('kombu_stomp.transport.QoS._stomp_ack')
    def test_ack__delegates_to_stomp_ack(self, stomp_ack):
        self.qos.ack(self.delivery_tag)
        stomp_ack.assert_called_once_with(self.delivery_tag)

    def test_stomp_ack(self):
        # next line is requierd because we are not calling append first
        self.qos.ids[self.delivery_tag] = self.msg_id
        self.qos._stomp_ack(self.delivery_tag)

        conn = self.channel.conn_or_acquire.return_value.__enter__.return_value
        conn.ack.assert_called_once_with(self.msg_id)

    def test_stomp_ack__no_sg_id(self):
        self.qos._stomp_ack(self.delivery_tag)
        self.assertFalse(self.channel.conn_or_acquire.called)


class ChannelConnectionTests(unittest.TestCase):
    def setUp(self):
        self.userid = 'user'
        self.passcode = 'pass'
        self.connection = mock.Mock(**{
            'client.transport_options': {},
            'client.userid': self.userid,
            'client.password': self.passcode,
        })
        self.channel = transport.Channel(connection=self.connection)
        self.queue = 'queue'

    @mock.patch('kombu_stomp.stomp.Connection')
    def test_conn_or_acquire__return_context_manager(self, Connection):
        with self.channel.conn_or_acquire() as conn:
            self.assertEqual(conn, Connection.return_value)

        self.assertEqual(conn, Connection.return_value)

    @mock.patch('kombu_stomp.stomp.Connection')
    def test_conn_or_acquire__start_conn_if_not_connected(self, Connection):
        Connection.return_value.is_connected.return_value = False
        with self.channel.conn_or_acquire() as conn:
            pass

        conn.start.assert_called_once_with()
        #conn.disconnect.assert_called_once_with()

    @mock.patch('kombu_stomp.stomp.Connection')
    def test_conn_or_acquire__connect_if_not_connected(self, Connection):
        Connection.return_value.is_connected.return_value = False
        with self.channel.conn_or_acquire() as conn:
            pass

        conn.connect.assert_called_once_with(
            username=self.userid,
            passcode=self.passcode,
            wait=True,
        )

    @mock.patch('kombu_stomp.stomp.Connection')
    def test_conn_or_acquire__do_not_disconnect(self, Connection):
        Connection.return_value.is_connected.return_value = False
        with self.channel.conn_or_acquire() as conn:
            pass

        self.assertFalse(conn.disconnect.called)

    @mock.patch('kombu_stomp.stomp.Connection')
    def test_conn_or_acquire__do_disconnect_on_demmand(self, Connection):
        Connection.return_value.is_connected.return_value = False
        with self.channel.conn_or_acquire(True) as conn:
            pass

        conn.disconnect.assert_called_once_with()

    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_get_many(self, conn_or_acquire):
        stomp_conn = conn_or_acquire.return_value.__enter__.return_value
        iterator = stomp_conn.message_listener.iterator
        iterator.return_value = iter([1])

        self.assertEqual(self.channel._get_many([self.queue]), 1)
        iterator.assert_called_once_with(timeout=None)

    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_put(self, conn_or_acquire):
        message = {'body': 'body'}
        stomp_conn = conn_or_acquire.return_value.__enter__.return_value

        self.channel._put(self.queue, message)

        stomp_conn.send.assert_called_once_with(
            '/queue/{0}'.format(self.queue),
            'body'
        )

    @mock.patch('kombu.transport.virtual.Channel.queue_bind')
    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_queue_bind__calls_super(self, conn_or_acquire, queue_bind):
        self.channel.queue_bind(self.queue)

        queue_bind.assert_called_once_with(self.queue, None, '', None)

    @mock.patch('kombu.transport.virtual.Channel.queue_bind')
    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_queue_bind__subscribe(self, conn_or_acquire, queue_bind):
        stomp_conn = conn_or_acquire.return_value.__enter__.return_value
        self.channel.queue_bind(self.queue)

        stomp_conn.subscribe.assert_called_once_with(
            '/queue/{0}'.format(self.queue),
            ack='client-individual',
        )

    @mock.patch('kombu.transport.virtual.Channel.queue_unbind')
    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_queue_unbind__calls_super(self, conn_or_acquire, queue_unbind):
        self.channel.queue_unbind(self.queue)

        queue_unbind.assert_called_once_with(self.queue, None, '', None)

    @mock.patch('kombu.transport.virtual.Channel.queue_unbind')
    @mock.patch('kombu_stomp.transport.Channel.conn_or_acquire',
                new_callable=mock.MagicMock)  # for the context manager
    def test_queue_bind__unsubscribe(self, conn_or_acquire, queue_unbind):
        stomp_conn = conn_or_acquire.return_value.__enter__.return_value
        self.channel.queue_unbind(self.queue)

        stomp_conn.unsubscribe.assert_called_once_with(
            '/queue/{0}'.format(self.queue)
        )

    @mock.patch('kombu.transport.virtual.Channel.close')
    @mock.patch('kombu_stomp.stomp.Connection')
    def test_close__call_super(self, Connection, close):
        self.channel.close()

        close.assert_called_once_with()

    @mock.patch('kombu.transport.virtual.Channel.close')
    @mock.patch('kombu_stomp.stomp.Connection')
    def test_close__disconnect(self, Connection, close):
        self.channel.close()

        Connection.return_value.disconnect.assert_called_once_with()

    @mock.patch('kombu.transport.virtual.Channel.close')
    @mock.patch('kombu_stomp.stomp.Connection')
    def test_close__close_closed_connection(self, Connection, close):
        Connection.close.side_effect = exc.NotConnectedException
        self.channel.close()  # just check this doesn't trigger exceptions

    def test_queue_destination__prefix(self):
        self.connection.client.transport_options = {
            'queue_name_prefix': 'prefix.',
        }

        self.assertEqual(
            self.channel.queue_destination(self.queue),
            '/queue/prefix.queue',
        )
