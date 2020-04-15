#!/bin/bash

curl -H'Content-Type: application/json' -XPOST 'localhost:9200/benchmarks/1/_delete_by_query?conflicts=proceed' -d' { "query": { "match_all": {} }}'
