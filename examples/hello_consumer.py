from __future__ import print_function
import logging
import os

import kombu_stomp
kombu_stomp.register_transport()

from kombu import Connection

if os.environ.get('DEBUG', False):
    logging.basicConfig(level=logging.DEBUG)


with Connection(os.environ['CONN_STR']) as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    message = simple_queue.get(block=True, timeout=60)
    print("Received: %s" % message.payload)
    message.ack()
    simple_queue.close()
