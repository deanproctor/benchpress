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

from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
    return hook

@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
def test_directory_to_trash(sdc_builder, sdc_executor, number_of_threads, benchmark_args):
    dataset = benchmark_args['DATASET']
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             delimiter_format_type='CUSTOM',
                             delimiter_character=benchpress.datasets[dataset]['delimiter'],
                             file_name_pattern=benchpress.datasets[dataset]['file_pattern'] + '*', file_name_pattern_mode='GLOB',
                             files_directory='/resources/' + benchpress.dataset_dir,
                             number_of_threads=number_of_threads,
                             batch_size_in_recs=1000)
    trash = pipeline_builder.add_stage('Trash')
    directory >> trash
    pipeline = pipeline_builder.build()
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=benchpress.record_count, runs=3)
    benchpress.save_results(results, sdc_builder.version, 'Directory', 'Trash', number_of_threads, dataset)

@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
def test_directory_to_localfs(sdc_builder, sdc_executor, number_of_threads, benchmark_args):
    dataset = benchmark_args['DATASET']
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             delimiter_format_type='CUSTOM',
                             delimiter_character=benchpress.datasets[dataset]['delimiter'],
                             file_name_pattern=benchpress.datasets[dataset]['file_pattern'] + '*', file_name_pattern_mode='GLOB',
                             files_directory='/resources/' + benchpress.dataset_dir,
                             number_of_threads=number_of_threads,
                             batch_size_in_recs=1000)
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='DELIMITED',
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', files_suffix='csv', max_records_in_file=0)
    stop_stage = pipeline_builder.add_stop_event_stage('Shell')
    stop_stage.set_attributes(script='rm -rf ' + tmp_directory)
    directory >> local_fs
    pipeline = pipeline_builder.build()
    results = sdc_executor.benchmark_pipeline(pipeline, record_count=benchpress.record_count, runs=3)
    benchpress.save_results(results, sdc_builder.version, 'Directory', 'Local FS', number_of_threads, dataset)

