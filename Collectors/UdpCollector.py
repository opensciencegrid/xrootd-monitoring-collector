#!/usr/bin/env python

"""
An abstract class for listening for data from a remote XRootD host via
UDP, performing simple aggregations, and forwarding the resulting data
to a data store.
"""

import json
import logging
import logging.config
import re
import socket
import struct
import sys
import time

from multiprocessing import Process, Queue

import six
from six.moves import configparser

import pika
import decoding
import wlcg_converter

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('DetailedCollector')

if len(sys.argv) != 2:
    logger.error('Usage: DetailedCollector.py <connection.conf>')
    sys.exit(1)
connect_config = configparser.ConfigParser()
connect_config.read(sys.argv[1])

DETAILED_PORT = 9930

sock.bind(("0.0.0.0", DETAILED_PORT))

class _LoggerWriter(object):
    """
    Set stdout / stderr to the logger
    https://stackoverflow.com/questions/19425736/how-to-redirect-stdout-and-stderr-to-logger-in-python
    """

    def __init__(self, level):
        # self.level is really like using log.debug(message)
        # at least in my case
        self.level = level

    def write(self, message):
        """
        if statement reduces the amount of newlines that are
        printed to the logger
        """
        if message != '\n':
            self.level(message)

    def flush(self):
        """
        create a flush method so things can be flushed when
        the system wants to. Not sure if simply 'printing'
        sys.stderr is the correct way to do it, but it seemed
        to work properly for me.
        """
        self.level(sys.stderr)


class UdpCollector(object):

    DEFAULT_HOST = '0.0.0.0'
    DEFAULT_PORT = None


    def __init__(self, config, bind_addr):
        self.channel = None
        self.bind_addr = bind_addr
        self.socket = None
        self.config = config
        self.message_q = None


    def _create_rmq_channel(self):
        """
        Create a fresh connection to RabbitMQ
        """
        parameters = pika.URLParameters(connect_config.get('AMQP', 'url'))
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()


    def _init_logging(self):
        daemon_dir = os.path.split(sys.argv[0])[0]
        logging_conf = os.path.join(daemon_dir, "logging.conf")
        logging.config.fileConfig(logging_conf)
        self.logger = logging.getLogger('DetailedCollector')
        sys.stdout = _LoggerWriter(self.logger.debug)
        sys.stderr = _LoggerWriter(self.logger.error)


    def _shutdown_child(self):
       if self._child_process.join(1) is None:
           self._child_process.terminate()
           if self._child_process.join(1) is None:
               self._child_process.kill()
               self._child_process.join()
        self._child_process.close()


    def _launch_child(self):
        if self._child_process
            self._shutdown_child()
        self._child_process = Process(target=self._start_child, args=(self.config, self.message_q))
        self._child_process.name = "Collector processing thread"
        self._child_process.daemon = True
        self._child_process.start()


    def start(self):
        """
        Start processing events from the UDP socket and send to the child process.
        """
        self._create_rmq_channel()

        self.message_q = Queue()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024*4)
        self.sock.bind(self.bind_addr)

        self._launch_child()

        try:
            n_messages = 0
            last_message = time.time()
            while True:
                rlist, wlist, xlist = select.select([self.sock, self._child_process.sentinel], [], [], 10)
                if self.sock in rlist:
                    message, addr = sock.recvfrom(65536)

                    message_q.put([message, addr[0], addr[1]])
                    n_messages += 1
                    if n_messages % 10000 == 0:
                        self.logger.info("Current UDP packets processed count: %i", n_messages)
                    if time.time() - last_message >= 10:
                        self.logger.info("Current UDP packets processed count: {}".format(n_messages)
                        last_message += 10 
                elif self._child_process.sentinel in rlist:
                    self.logger.error("Child event process died; restarting")
                    self._launch_child()
        finally:
            self.message_q.put(None)
            self._shutdown_child()


    @classmethod
    def start_child(Collector, config, message_q):
        coll = Collector(config)
        coll._init_logging()
        coll.message_q = message_q
        coll.run()


    @abstractmethod
    def process(self, d, addr, port):
        """
        Function invoked each time a new UDP packet is available.
        """


    def run(self, message_q):

        last_warning = time.time()
        while True:
            try:
                info = message_q.get(True, 10)
            except queue.Empty:
                logging.info('No messages parsed in the last 10 seconds.')
                continue

            if len(info) == 3:
                self.process(*info)
            else:
                break

            if message_q.qsize() > 200:
                if time.time() - last_warning > 5:
                    logger.error('QSize is large: %d', message_q.qsize())
                    last_warning = time.time()


    @classmethod
    def main(Collector, port=None, host=None):
        """
        Main driver for the daemon.
        """
        if port is None:
            port = Collector.DEFAULT_PORT
            if port is None:
                print("Collector implementation must provide a default port", file=sys.stderr)
                sys.exit(1)
        if host is None:
            host = Collector.DEFAULT_HOST
    
        argparse.ArgumentParser()
        argparse.add_argument("config", required=True, "Location of configuration file.")
        args = argparse.parse_args()

        config = configparser.ConfigParser()
        config.read(args.config)

        coll = Collector(config, bind_addr=(host, port))
