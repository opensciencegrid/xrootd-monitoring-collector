import base64
import json
import logging
import multiprocessing
import socket
import stomp


class StompyListener(stomp.ConnectionListener):
    def __init__(self, message_q: multiprocessing.Queue, metrics_q: multiprocessing.Queue):
        self.message_q = message_q
        self.metrics_q = metrics_q

    def on_error(self, frame):
        logging.error('Received an error "%s"' % frame.body)

    def on_message(self, headers, body):
        # Parse the JSON message
        loaded_json = json.loads(body)

        # Base64 decode the data
        message = base64.standard_b64decode(loaded_json['data'])

        # Send to message queue
        # Get the address and port
        addr = loaded_json['remote'].rsplit(":", 1)
        self.message_q.put([message, addr[0], addr[1]])

        # Update the number of messages received on the bus to the metrics_q
        self.metrics_q.put({'type': 'pushed messages', 'count': 1})

    def on_disconnected(self):
        logging.debug('disconnected')


def get_stomp_connection_objects(mb_alias, port, username, password,
                                 connections=[], message_q=[], metrics_q=[]):
    # If we had connection already, make sure to close them
    for connection in connections:
        if connection.is_connected():
            connection.close()

    connections = []
    # Following advice from messaging team URL should be translated into all possible
    # hosts behind the alias
    # Get the list of IPs behind the alias
    hosts  = socket.gethostbyname_ex(mb_alias)[2]

    host_and_ports   = map(lambda x: (x, port), hosts)
    # Create a connection to each broker available
    for host_and_port in host_and_ports:
        connection = stomp.Connection(host_and_ports=[host_and_port])
        connection.set_listener('StompyListener', StompyListener(message_q, metrics_q))
        connection.connect(username=username, passcode=password, wait=True)
        connections.append(connection)
        
    return connections