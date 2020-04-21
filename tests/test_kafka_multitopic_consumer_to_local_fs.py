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
import pytest
import benchpress

from streamsets.testframework.markers import database, cluster

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
    return hook

@pytest.mark.parametrize('origin', ('Kafka Multitopic Consumer',))
@pytest.mark.parametrize('destination', ('Local FS',))
@pytest.mark.parametrize('dataset', ('narrow','wide'))
@pytest.mark.parametrize('number_of_threads', (1,2,4,8))
@pytest.mark.parametrize('batch_size', (1000,))
@pytest.mark.parametrize('destination_format', ('DELIMITED',))
@pytest.mark.parametrize('num_processors', (0,4))
@database
@cluster('kafka')
def test_benchpress(sdc_builder, sdc_executor, origin, destination, dataset, number_of_threads, batch_size, destination_format, num_processors, benchmark_args, database, cluster):
    benchpress.run_test(sdc_builder, sdc_executor, origin, destination, dataset, number_of_threads, batch_size, destination_format, num_processors, benchmark_args, database_env=database, kafka_env=cluster)

