from __future__ import print_function
import logging
import os

import kombu_stomp
kombu_stomp.register_transport()

from kombu import Connection

if os.environ.get('DEBUG', False):
    logging.basicConfig(level=logging.DEBUG)


with Connection(os.environ['CONN_STR']) as conn:
    with conn.SimpleQueue('simple_queue') as queue:
        # queue.put({'hello': 'world'}, serializer='json', compression='zlib')
        queue.put({'hello': 'world'}, serializer='json')
