#!/bin/bash

ELASTICSEARCH='https://vpc-benchpress-whgyccujkszqd7kxu6cqncxlly.us-west-2.es.amazonaws.com/benchmarks'

curl -H'Content-Type: application/json' -XDELETE $ELASTICSEARCH -d' { "query": { "match_all": {} }}'
