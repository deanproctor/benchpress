#!/bin/bash

versions=(3.10.0 3.10.1 3.10.2 3.11.0 3.12.0 3.13.0 3.14.0)
#versions=(3.14.0)
runs=1

mkdir -p results resources

for dataset in census cardtxn; do
  if [[ ! -f resources/$dataset ]]; then
      wget -qO - https://benchpress.s3-us-west-2.amazonaws.com/datasets/$dataset.tgz | tar xzvf - -C resources/ 
      touch resources/$dataset
  fi
done

for version in ${versions[@]}; do
    for test in `ls tests/test_*.py`; do
	case $test in
            "tests/test_directory.py")
                stf --sdc-resources-directory ./resources test -sv --sdc-version $version --database postgresql://postgres.cluster:5432/default --benchmark-arg=KEEP_TABLE=true --benchmark-arg=RUNS=$runs $test
                ;;

            "tests/test_jdbc_multitable_consumer.py")
	        ste start PostgreSQL_9.6.2
                stf --sdc-resources-directory ./resources test -sv --sdc-version $version --database postgresql://postgres.cluster:5432/default --benchmark-arg=KEEP_TABLE=true --benchmark-arg=RUNS=$runs $test 
                ;;
        esac
	# Upload the test results to Elasticsearch.  
	# Maintain the results files until the next test so we can manually inspect them.
        for file in `ls results/*.json`; do 
            echo curl -H 'Content-Type: application/json' -XPOST 'http://localhost:9200/benchmarks/1' -d @$file
        done
    done
done

