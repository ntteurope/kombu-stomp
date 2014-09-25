from __future__ import print_function
import datetime
import logging
import os

import kombu_stomp
kombu_stomp.register_transport()

from kombu import Connection

if os.environ.get('DEBUG', False):
    logging.basicConfig(level=logging.DEBUG)


with Connection(os.environ['CONN_STR']) as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    message = 'helloword, sent at %s' % datetime.datetime.today()
    simple_queue.put(message)
    print('Sent: %s' % message)
    simple_queue.close()
