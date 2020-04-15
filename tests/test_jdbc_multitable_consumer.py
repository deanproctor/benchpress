# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""
import csv
import json
import logging
import os
import pytest
import statistics
import string
import sqlalchemy
import tempfile

from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def test_table(database, benchmark_args):
    datasets = {'sales': {'file': './resources/sales_50k.csv', 'delimiter': '|'},
                'census': {'file': './resources/census_50k.csv', 'delimiter': ','}}
    table_name = benchmark_args.get('DATASET') or get_random_string()
    data = (read_csv_as_json(datasets[benchmark_args['DATASET']]['file'], datasets[benchmark_args['DATASET']]['delimiter']))
    logger.info('Inserting data into database table %s ...', table_name)
    database.insert_data(table_name, data)
    yield table_name
    if not benchmark_args.get('KEEP_TABLE'):
        database.delete_table(table_name)

@pytest.mark.parametrize('record_count', (50_000,))
@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_trash(sdc_builder, sdc_executor, database, test_table, record_count, number_of_threads, benchmark_args):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=10)
    save_results(results, sdc_builder.version, 'JDBC Multitable Consumer', 'Trash', record_count, number_of_threads, test_table)

@pytest.mark.parametrize('record_count', (50_000,))
@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_localfs(sdc_builder, sdc_executor, database, test_table, record_count, number_of_threads, benchmark_args):
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='JSON',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='json', max_records_in_file=100000)
    jdbc_multitable_consumer >> local_fs
    pipeline = pipeline_builder.build().configure_for_environment(database)
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=10)
    save_results(results, sdc_builder.version, 'JDBC Multitable Consumer', 'Local FS', record_count, number_of_threads, test_table)

def save_results(results, sdc_version, origin, destination, record_count, threads, dataset):
    """ Persists benchmark results as a JSON file"""
    results['sdc_version'] = sdc_version
    results['origin'] = origin
    results['destination'] = destination
    results['record_count'] = record_count
    results['threads'] = threads
    results['dataset'] = dataset

    # Remove outliers
    results['runs'] = [x for x in results['runs'] if -1 < (x - results['throughput_mean']) / results['throughput_std_dev'] < 1]
    results['throughput_mean'] = statistics.mean(results['runs'])

    with open("results/" + results['pipeline_title'] + ".json", "w") as file:
        json.dump(results, file)

def read_csv_as_json(file_path, delimiter):
    """ Reads a csv file with records separated by delimiter"""
    with open(file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
        rows = list(csv_reader)
    return rows
