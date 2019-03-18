## Setup docker

1. Setup docker for kafka cluster & Redis
```bash
docker-compose up kafka-cluster
```

2. Run docker for kafka cluster
```bash
docker run --rm -it -v "$(pwd)":/data --net=host landoop/fast-data-dev bash
```

## Setup Kafka Connect
3. Build connector

## Setup Kafka
4. Create topic in kafka with 3 partitions
```bash
kafka-topics --create --topic events-standalone --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

5. Generate source data
```bash
python3 botgen_line.py -b 1 -u 1000 -n 100 -d 300 -f data/data_log.json
```

6. Load source data to Kafka topic with Kafka Connect
```bash
connect-standalone worker.properties file-stream.properties
```