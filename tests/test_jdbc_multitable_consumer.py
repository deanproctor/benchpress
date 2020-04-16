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
import sqlalchemy
import statistics
import string
import tempfile
from utils import save_results

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
    datasets = {'sales': {'file': '/resources/resources/sales_1m.csv', 'delimiter': '|'},
                'census': {'file': '/resources/resources/census_50m.csv', 'delimiter': ','}}
    table_name = benchmark_args.get('DATASET') or get_random_string()
    engine = database.engine
    if not engine.dialect.has_table(engine, table_name):
        with open('./resources/' + os.path.basename(datasets[table_name]['file'])) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=datasets[table_name]['delimiter'])
            header = []
            header.append(next(csv_reader))
            database.insert_data(table_name, header)
        pipeline_builder = sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format='DELIMITED', 
                                 header_line='WITH_HEADER', 
                                 delimiter_format_type='CUSTOM',
                                 delimiter_character=datasets[table_name]['delimiter'],
                                 file_name_pattern=os.path.basename(datasets[table_name]['file']), file_name_pattern_mode='GLOB',
                                 files_directory=os.path.dirname(datasets[table_name]['file']),
                                 batch_size_in_recs=10000)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
        jdbc_producer.set_attributes(default_operation="INSERT",
                                     field_to_column_mapping=[],
                                     enclose_object_names = True,
                                     use_multi_row_operation = True,
                                     table_name=table_name)

        directory >> jdbc_producer

        populate_pipeline = pipeline_builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(populate_pipeline)
        sdc_executor.start_pipeline(populate_pipeline).wait_for_pipeline_output_records_count(50000000, timeout_sec=3600)
        sdc_executor.stop_pipeline(populate_pipeline)
        sdc_executor.remove_pipeline(populate_pipeline)    
    yield table_name
    if not benchmark_args.get('KEEP_TABLE'):
        database.delete_table(table_name)

@pytest.mark.parametrize('record_count', (50_000_000,))
@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_trash(sdc_builder, sdc_executor, database, test_table, record_count, number_of_threads, benchmark_args):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table,partitioningMode='BEST_EFFORT',maxNumActivePartitions=2*number_of_threads)],
                                            number_of_threads=number_of_threads,
                                            maximum_pool_size=number_of_threads)
    trash = pipeline_builder.add_stage('Trash')
    jdbc_multitable_consumer >> trash
    pipeline = pipeline_builder.build().configure_for_environment(database)
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=10)
    save_results(results, sdc_builder.version, 'JDBC Multitable Consumer', 'Trash', record_count, number_of_threads, test_table)

@pytest.mark.parametrize('record_count', (50_000_000,))
@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@database
def test_jdbc_multitable_to_localfs(sdc_builder, sdc_executor, database, test_table, record_count, number_of_threads, benchmark_args):
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=test_table,partitioningMode='BEST_EFFORT',maxNumActivePartitions=2*number_of_threads)],
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

