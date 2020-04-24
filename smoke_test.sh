#!/bin/bash

# Benchmark Arguments
RUNS=1
RECORD_COUNT=1

ELASTICSEARCH='https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/benchmarks/1'
BENCHMARK_ARGS="--benchmark-arg=RUNS=${RUNS} --benchmark-arg=RECORD_COUNT=${RECORD_COUNT}"

# Data Collector versions to test
#VERSIONS=(3.14.0 3.13.0 3.12.0 3.11.0 3.10.2 3.10.1 3.10.0)
VERSIONS=(3.14.0)
TESTS=(tests/test_jdbc_multitable_consumer_origin.py tests/test_kafka_multitopic_consumer_origin.py tests/test_s3_origin.py tests/test_sftp_client_origin.py tests/test_directory_origin.py)

# Environments
HTTP=HTTP
JDBC=PostgreSQL_9.6.2
#JDBC=Oracle_11_2_0
KAFKA=Kafka_1.0
SFTP="SFTP -v $(pwd)/resources:/home/admin/sftp_dir"

AWS_ARGS="--aws-s3-region-name us-west-2 --aws-s3-bucket-name benchpress"
HTTP_ARGS="--http-server-url http://myhttpmockserver:8000"
JDBC_ARGS="--database postgresql://postgres.cluster:5432/default"
#JDBC_ARGS="--database oracle://ora_11_2_0.cluster:1521/XE"
KAFKA_ARGS="--cluster-server kafka://node-1.cluster:9092,node-2.cluster:9092,node-3.cluster:9092 --kafka-version 1.0.0 --kafka-zookeeper node-1.cluster:2181,node-2.cluster:2181,node-3.cluster:2181 --confluent-schema-registry http://registry-1.cluster:8081"
SFTP_ARGS="--sftp-url sftp://mysftpserver:22/sftp_dir --sftp-username admin --sftp-password admin"

DOCKER_ARGS="--docker-image streamsets/testframework:sdk-31361_31360"

# Create test directories
mkdir -p results/sent resources

# Download the data sets
for dataset in census cardtxn; do
  if [[ ! -f resources/$dataset ]]; then
      wget -qO - https://benchpress.s3-us-west-2.amazonaws.com/datasets/$dataset.tgz | tar xzvf - -C resources/ 
      touch resources/$dataset
  fi
done

# Start the test environments
ste restart $HTTP
ste start $JDBC
ste start $KAFKA
ste restart $SFTP

for version in ${VERSIONS[@]}; do
    for test in ${TESTS[@]}; do 
        stf -v $DOCKER_ARGS --sdc-resources-directory ./resources test -sv -rs --sdc-version $version $AWS_ARGS $HTTP_ARGS $JDBC_ARGS $KAFKA_ARGS $SFTP_ARGS $BENCHMARK_ARGS $test
    done
done

