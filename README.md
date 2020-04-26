# Benchpress
Benchpress is a project for generating benchmark performance data for StreamSets Data Collector.

**[Click here to view benchmark results.](https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/_plugin/kibana/app/kibana#/dashboard/a18eed40-8337-11ea-96b6-cd28f22d3a55)**

There are 4 primary components to Benchpress:
1. **[benchpress.py](tests/benchpress.py)**: a class providing a library of parameterized stage configurations defined using the StreamSets SDK along with automation to dynamically generate and execute pipeline benchmarks based on test parameter permutations.
2. **[tests](tests)**: STF test definitions, grouped by origin stage.  The test definitions use Pytest parameters to generate the pipeline permutations to be benchmarked.
3. **[bin/benchpress](bin/benchpress)**: a wrapper script used to configure STE environments and launch the STF benchmarks.
4. A **[Kibana dashboard](https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/_plugin/kibana/app/kibana#/dashboard/a18eed40-8337-11ea-96b6-cd28f22d3a55)** provides frontend visualization of benchmark results.

## Obtaining Benchmark Results
A common repository of benchmark results is maintained in an AWS Elasticsearch cluster hosted in the SE Basis account.

The Elasticsearch server and Kibana dashboard are not publicly available, so you will need access to the AWS Basis bastion host in order to view results.  Contact DevOps if you do not already have access to the bastion host.

Configure SwitchyOmega in your browser to proxy the following host via the Basis bastion server: 

    vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com
    
## Running Benchpress
The bin/benchpress script configures STE environments on the local host and with default parameters runs the full suite of benchmarks against several versions of SDC.  The script accepts several parameters:

~~~
$ ./bin/benchpress -h
Usage: benchpress [-h] [-i <iterations per test>] [-r <record count>] [-s "<SDC version(s)>"] [-t "<test file(s)>"]
                      [-u <upload results - yes/no>] [-e "<Elasticsearch URL>"
                      [-j <JDBC STE environment>] [-J "<JDBC STE arguments>"]
                      [-c <cluster STE environment>] [-C "<cluster STE arguments>"]

    -h    Display this help

    -i    The number of iterations per test permutation to run
              Default: 3

    -r    The number of records to use per test
              Default: 15000000

    -s    The SDC version(s) to test.  Use quotes around multiple versions
              Example: -s "3.14.0 3.13.0"
              Default: "3.14.0 3.13.0 3.12.0 3.11.0 3.10.2 3.10.1 3.10.0"

    -t    The specific test files to run.  Use quotes around multiple tests
              Example: -t "tests/test_directory_origin.py tests/test_s3_origin.py"
              Default: all tests

    -u    Whether to upload test results to Elasticsearch
              Default: yes

    -e    The Elasticsearch index URL to use for uploading results
              Default: https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/benchmarks/1

    -j    The STE environment to start for JDBC tests
              Default: PostgreSQL_9.6.2

    -J    The STE JDBC environment arguments to pass to STF.  Use quotes around the arguments
              Default: "--database postgresql://postgres.cluster:5432/default"

    -c    The STE cluster environment to start for Hadoop and Kafka tests
              Default: Kafka_1.0

    -C    The STE cluster environment arguments to pass to STF.  Use quotes around the arguments
              Default: "--cluster-server kafka://node-1.cluster:9092,node-2.cluster:9092,node-3.cluster:9092 
                        --kafka-version 1.0.0 --kafka-zookeeper node-1.cluster:2181,node-2.cluster:2181,node-3.cluster:2181 
                        --confluent-schema-registry http://registry-1.cluster:8081"

~~~
