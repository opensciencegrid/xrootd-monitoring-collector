XRootD Monitoring Collector
===========================

This software package contains the code necessary for ingesting monitoring data from the XRootD server,
aggregating it into one monitoring record per file transfer, and sending a resulting JSON-formatted
record into a AMQP-based message bus.


Configuration
-------------

The DetailedCollector needs the a configuration in order to connect to the AMQP message bus.

    [AMQP]

    # Host information
    url = amqps://username:password@example.com

    # Exchange to write to
    exchange = xrd.detailed 

This file is named `connection.conf` and should be in the Collectors directory.

