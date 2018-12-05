1. Setup docker for kafka cluster 

```bash
docker-compose up kafka-cluster
```

2. Run docker for kafka cluster
```bash
docker run --rm -it -v "$(pwd)":/data --net=host landoop/fast-data-dev bash
```

3. Create topic in kafka with 3 partitions
```bash
kafka-topics --create --topic events-standalone --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

4. Generate source data

```bash
python3 botgen_line.py -b 1 -u 1000 -n 100 -d 300 -f data/data_log.json
```

5. Load source data to Kafka topic with Kafka Connect
```bash
connect-standalone worker.properties file-stream.properties
```