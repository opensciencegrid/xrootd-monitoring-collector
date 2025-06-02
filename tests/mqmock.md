
Retrieving a set of records from RabbitMQ
```
curl 'https://xrd-rabbitmq.gracc-prod.chtc.io/api/queues/xrd-mon/xrd.push.external/get' \
  -X POST \
  --user user:hunter2 \
  --data-raw '{"vhost":"xrd-mon","name":"xrd.push.external","truncate":"50000","ackmode":"ack_requeue_true","encoding":"auto","count":"1000"}' \
  --output records.out
```

Pull the payload fields and make into a JSON Lines file
```
jq --raw '.[].payload' records.out > test1.jsonl
```

Configure collector to use the mock MQ
```
[AMQP]
url = amqp://user:password@mock-mq
```

Start the services, including the mock MQ container
```
docker compose --profile mock up
```

Exec into the collector container and send test data to the mock MQ
```
docker compose exec -it detailed_collector /bin/sh
/tests/mqmock.py /tests/test1.jsonl
```
