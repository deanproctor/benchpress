#!/bin/bash

versions=(3.10.0 3.10.1 3.10.2 3.11.0 3.12.0 3.13.0 3.14.0)
#versions=(3.14.0)

mkdir -p results

for version in ${versions[@]}; do
    for test in `ls tests/*.py`; do
	# Clean up old results
        rm -f results/*.json

	# Todo: add case logic once we have multiple test files to set appropriate stf flags and ste
	# Starting and stopping postgres container to ensure the same initial state between tests
	ste start PostgreSQL_9.6.2
        stf --sdc-resources-directory ./resources test -sv --sdc-version $version --database postgresql://postgres.cluster:5432/default --benchmark-arg=KEEP_TABLE=true --benchmark-arg=DATASET=census $test 
        stf --sdc-resources-directory ./resources test -sv --sdc-version $version --database postgresql://postgres.cluster:5432/default --benchmark-arg=KEEP_TABLE=true --benchmark-arg=DATASET=sales $test 
	#ste stop PostgreSQL_9.6.2

	# Upload the test results to Elasticsearch.  
	# Maintain the results files until the next test so we can manually inspect them.
        for file in `ls results/*.json`; do 
            curl -H 'Content-Type: application/json' -XPOST 'http://localhost:9200/benchmarks/1' -d @$file
        done
    done
done

