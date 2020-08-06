XRootD Monitoring Collector
===========================

XRootD monitoring collector for ingesting monitoring data from the XRootD server,
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

This file is named `connection.conf` and should be in the Collectors directory or deployed with docker volumes, as shown below.

Deployment
----------

The Detailed collector is available from DockerHub: https://hub.docker.com/repository/docker/opensciencegrid/xrootd-monitoring-collector

You can deploy this monitoring collector with docker-compose, or your favorite container orchestration engine.  It will need to receive UDP packets from XRootD servers.

Here is an example `docker-compose.yml` file for the Detailed Collector:

```
version: '3.2'
services:
  detailed_collector:
    image: "opensciencegrid/xrootd-monitoring-collector"
    volumes:
      - ./connection.conf:/configs/connection.conf
    ports:
      - "9930:9930/udp"
      - "8000:8000/tcp"
    restart: always
```

Monitoring
----------

The collector exports a [Prometheus](https://prometheus.io/) compatible interface on port 8000 that gives information about the internal state of the collector, including:

* Number of packets received
* Number of WLCG or StashCache file transfers sent to the AMQP message bus.
* Errors while processing the monitoring packets



