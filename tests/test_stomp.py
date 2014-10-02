from six.moves import queue

from kombu_stomp import stomp
from kombu_stomp.utils import mock
from kombu_stomp.utils import unittest


class ListenerTestCase(unittest.TestCase):
    def setUp(self):
        self.queue = mock.Mock()
        self.timeout = 5
        self.listener = stomp.MessageListener(q=self.queue)


class MessageListenerTests(ListenerTestCase):
    def setUp(self):
        super(MessageListenerTests, self).setUp()
        self.headers = {
            'content-type': 'application/json',
            'message-id': 'ID:services-55311-1412009732901-5:6816:-1:1:1',
            'content-encoding': 'utf-8',
            'properties': """{
    'body_encoding': u'base64',
        u'delivery_info': {
            u'priority': 0,
            'routing_key': u'simple_queue',
            'exchange': u'simple_queue'
        },
        'delivery_mode': 2,
        'delivery_tag': '423e3830-e67a-458d-9aa0-f58df4d01639'
}""",

            'destination': '/queue/simple_queue',
            'timestamp': 1412068081608,
            'expires': 0,
            'priority': 4,
        }
        self.body = 'eyJoZWxsbyI6ICJ3b3JsZCJ9'
        self.maxDiff = None

    @mock.patch('six.moves.queue.Queue')
    def test_default_queue(self, Queue):
        stomp.MessageListener()
        Queue.assert_called_once_with()

    def test_on_message__transforms_the_message_to_kombu_format(self):
        with mock.patch.object(self.listener, 'to_kombu_message') as tokm:
            self.listener.on_message(self.headers, self.body)
        tokm.assert_called_once_with(self.headers, self.body)

    def test_on_message__puts_to_the_queue(self):
        with mock.patch.object(self.listener, 'to_kombu_message') as tokm:
            self.listener.on_message(self.headers, self.body)
        self.queue.put.assert_called_once_with(tokm.return_value)

    def test_to_kombu_message__return_message_as_dict(self):
        self.assertDictEqual(
            self.listener.to_kombu_message(self.headers, self.body)[0][0],
            {
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
                'body': self.body,
            }
        )

    def test_to_kombu_message__return_message_id(self):
        self.assertEqual(
            self.listener.to_kombu_message(self.headers, self.body)[0][1],
            'ID:services-55311-1412009732901-5:6816:-1:1:1',
        )

    def test_to_kombu_message__return_queue_name(self):
        self.assertEqual(
            self.listener.to_kombu_message(self.headers, self.body)[1],
            'simple_queue',
        )

    def test_iterator(self):
        self.queue.get.side_effect = (1, 3)
        it = self.listener.iterator(timeout=5)
        self.assertEqual(1, next(it))
        self.assertEqual(3, next(it))

    def test_iterator__empty(self):
        self.listener.q = queue.Queue()
        it = self.listener.iterator(timeout=None)
        self.assertRaises(queue.Empty, lambda: next(it))

    def test_iterator__non_blocking(self):
        self.queue.get.side_effect = [1]
        it = self.listener.iterator(None)
        next(it)
        self.queue.get.assert_called_once_with(block=False, timeout=None)

    def test_queue_from_destination(self):
        self.assertEqual(
            self.listener.queue_from_destination(self.headers['destination']),
            'simple_queue',
        )


class ConnectionTests(unittest.TestCase):

    @mock.patch('kombu_stomp.stomp.MessageListener')
    def test_message_lisetner_its_set_on_init(self, Listener):
        self.conn = stomp.Connection()
        self.assertEqual(
            self.conn.get_listener('message_listener'),
            Listener.return_value,
        )
        Listener.assert_called_once_with()
