#!/bin/bash

ELASTICSEARCH='https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/benchmarks/_delete_by_query'

curl -H'Content-Type: application/json' -XPOST $ELASTICSEARCH -d' { "query": { "range": {"record_count": {"lt": 15000000}} }}'
