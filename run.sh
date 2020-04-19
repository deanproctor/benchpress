#!/bin/bash

# Benchmark Arguments
RUNS=1
RECORD_COUNT=5000

BENCHMARK_ARGS="--benchmark-arg=RUNS=${RUNS} --benchmark-arg=RECORD_COUNT=${RECORD_COUNT}"

# Data Collector versions to test
#VERSIONS=(3.10.0 3.10.1 3.10.2 3.11.0 3.12.0 3.13.0 3.14.0)
VERSIONS=(3.14.0)

# Environments
JDBC=PostgreSQL_9.6.2
KAFKA=Kafka_1.0

JDBC_ARGS="--database postgresql://postgres.cluster:5432/default"
KAFKA_ARGS="--cluster-server kafka://node-1.cluster:9092,node-2.cluster:9092,node-3.cluster:9092 --kafka-version 1.0.0 --kafka-zookeeper node-1.cluster:2181,node-2.cluster:2181,node-3.cluster:2181 --confluent-schema-registry http://registry-1.cluster:8081"

# Create test directories
mkdir -p results resources

# Download the data sets
for dataset in census cardtxn; do
  if [[ ! -f resources/$dataset ]]; then
      wget -qO - https://benchpress.s3-us-west-2.amazonaws.com/datasets/$dataset.tgz | tar xzvf - -C resources/ 
      touch resources/$dataset
  fi
done

# Start the test environments
ste start $JDBC
ste start $KAFKA

for version in ${VERSIONS[@]}; do
    stf --sdc-resources-directory ./resources test -sv --sdc-version $version $JDBC_ARGS $KAFKA_ARGS $BENCHMARK_ARGS tests/test_directory.py
    stf --sdc-resources-directory ./resources test -sv --sdc-version $version $JDBC_ARGS $KAFKA_ARGS $BENCHMARK_ARGS tests/test_jdbc_multitable_consumer.py
done

# Save results to Elasticsearch
for file in `ls results/*.json`; do 
    echo curl -H 'Content-Type: application/json' -XPOST 'http://localhost:9200/benchmarks/1' -d @$file
done

