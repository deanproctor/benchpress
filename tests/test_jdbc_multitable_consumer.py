# Copyright 2017 StreamSets Inc.
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
import logging
import os
import pytest
import string
import tempfile
import benchpress

from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
    return hook

@pytest.fixture(scope='module')
@database
def test_table(database, benchmark_args, sdc_builder, sdc_executor):
    table_name = benchmark_args.get('DATASET')
    benchpress.load_table(database, sdc_builder, sdc_executor, table_name)
    yield table_name
    if not benchmark_args.get('KEEP_TABLE'):
        database.delete_table(table_name)

@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_trash(sdc_builder, sdc_executor, database, test_table, number_of_threads, benchmark_args):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table,partitioningMode='BEST_EFFORT',maxNumActivePartitions=2*number_of_threads)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=benchpress.record_count, runs=3)
    benchpress.save_results(results, sdc_builder.version, 'JDBC Multitable Consumer', 'Trash', number_of_threads, test_table)

@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_localfs(sdc_builder, sdc_executor, database, test_table, number_of_threads, benchmark_args):
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table,partitioningMode='BEST_EFFORT',maxNumActivePartitions=2*number_of_threads)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='DELIMITED',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv', max_records_in_file=0)
    stop_stage = pipeline_builder.add_stop_event_stage('Shell')
    stop_stage.set_attributes(script='rm -rf ' + tmp_directory)
    jdbc_multitable_consumer >> local_fs
    pipeline = pipeline_builder.build().configure_for_environment(database)
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=benchpress.record_count, runs=3)
    benchpress.save_results(results, sdc_builder.version, 'JDBC Multitable Consumer', 'Local FS', number_of_threads, test_table)

