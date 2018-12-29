#!/usr/bin/env python

"""
An abstract class for listening for data from a remote XRootD host via
UDP, performing simple aggregations, and forwarding the resulting data
to a data store.
"""

import abc
import argparse
import json
import logging
import logging.config
import multiprocessing
import os
import queue
import re
import select
import socket
import struct
import sys
import time

import six
from six.moves import configparser

import pika


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
        #self.level()


class UdpCollector(object):

    DEFAULT_HOST = '0.0.0.0'
    DEFAULT_PORT = None


    def __init__(self, config, bind_addr):
        self.channel = None
        self.bind_addr = bind_addr
        self.socks = []
        self.config = config
        self.message_q = None
        self.child_process = None
        self.exchange = config.get('AMQP', 'exchange')

    def _create_rmq_channel(self):
        """
        Create a fresh connection to RabbitMQ
        """
        parameters = pika.URLParameters(self.config.get('AMQP', 'url'))
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()


    def publish(self, routing_key, record: dict, retry=True, exchange=None):
        if exchange is None:
            exchange = self.exchange

        try:
            self.channel.basic_publish(exchange,
                                       routing_key,
                                       json.dumps(record),
                                       pika.BasicProperties(content_type='application/json',
                                       delivery_mode=pika.spec.TRANSIENT_DELIVERY_MODE))
        except Exception:
            if retry:
                self.logger.exception('Error while sending rabbitmq message; will recreate connection and retry')
                self._create_rmq_channel()
                self.publish(routing_key, record, retry=False)


    def _init_logging(self):
        daemon_dir = os.path.split(sys.argv[0])[0]
        logging_conf = os.path.join(daemon_dir, "logging.conf")
        logging.config.fileConfig(logging_conf)
        myName = self.__class__.__name__
        self.logger = logging.getLogger(myName)
        self.orig_stdout = sys.stdout
        sys.stdout = _LoggerWriter(logging.getLogger(myName + ".stdout").debug)
        self.orig_stderr = sys.stderr
        sys.stderr = _LoggerWriter(logging.getLogger(myName + ".stderr").error)


    def _shutdown_child(self):
       if self.child_process.join(1) is None:
           self.child_process.terminate()
           self.child_process.join()


    def _launch_child(self):
        if self.child_process:
            self._shutdown_child()
        self.child_process = multiprocessing.Process(target=self._start_child, args=(self.config, self.message_q))
        self.child_process.name = "Collector processing thread"
        self.child_process.daemon = True
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        sys.stdout = self.orig_stdout
        sys.stderr = self.orig_stderr
        try:
            self.child_process.start()
        finally:
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr


    def start(self):
        """
        Start processing events from the UDP socket and send to the child process.
        """
        self._init_logging()

        self.message_q = multiprocessing.Queue()

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024*4)
            sock.bind(self.bind_addr)
            self.socks.append(sock)
        except:
            self.logger.exception("Failed to create and bind to IPv4 UDP socket")
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024*4)
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            bind_addr = self.bind_addr
            if bind_addr[0] == '0.0.0.0':
                bind_addr = ('::', bind_addr[1])
            sock.bind(bind_addr)
            self.socks.append(sock)
        except:
            self.logger.exception("Failed to create and bind to IPv6 UDP socket")


        self._launch_child()

        try:
            n_messages = 0
            last_message = time.time()
            while True:
                sock_list = list(self.socks)
                sock_list.append(self.child_process.sentinel)
                rlist = multiprocessing.connection.wait(sock_list, timeout=10)
                if self.child_process.sentinel in rlist:
                    self.logger.error("Child event process died; restarting")
                    self._launch_child()
                for sock in self.socks:
                    if sock in rlist:
                        message, addr = sock.recvfrom(65536)

                        self.message_q.put([message, addr[0], addr[1]])
                        n_messages += 1
                        if n_messages % 10000 == 0:
                            self.logger.info("Current UDP packets processed count: %i", n_messages)
                if time.time() - last_message >= 10:
                    self.message_q.put(True)
                    self.logger.info("Current UDP packets processed count: {}".format(n_messages))
                    last_message += 10
        finally:
            self.message_q.put(None)
            self._shutdown_child()


    @classmethod
    def _start_child(Collector, config, message_q):
        coll = Collector(config, (Collector.DEFAULT_HOST, Collector.DEFAULT_PORT))
        coll._init_logging()
        coll._create_rmq_channel()
        coll.message_q = message_q
        try:
            coll.run()
        except KeyboardInterrupt:
            pass
        except:
            coll.logger.exception("Child process has failed:")


    @abc.abstractmethod
    def process(self, d, addr, port):
        """
        Function invoked each time a new UDP packet is available.
        """


    def run(self):

        last_warning = time.time()
        last_heartbeat = time.time()
        while True:
            now = time.time()
            if now - last_heartbeat > 20:
                logging.error("No heartbeat received in {} seconds.".format(now - last_heartbeat))
            try:
                info = self.message_q.get(True, 10)
            except queue.Empty:
                self.logger.info('No messages parsed in the last 10 seconds.')
                continue

            if info is True:
                last_heartbeat = time.time()
            elif info is None:
                break
            elif len(info) == 3:
                self.process(*info)

            if self.message_q.qsize() > 200:
                if time.time() - last_warning > 5:
                    logger.error('QSize is large: %d', self.message_q.qsize())
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
    
        parser = argparse.ArgumentParser()
        parser.add_argument("config", nargs=1, help="Location of configuration file.")
        args = parser.parse_args()

        config = configparser.ConfigParser()
        config.read(args.config[0])

        coll = Collector(config, bind_addr=(host, port))
        try:
            coll.start()
        except KeyboardInterrupt:
            coll.orig_stderr.write("Exiting on keyboard interrupt...\n")
            sys.exit(1)
