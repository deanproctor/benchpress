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
from streamsets.testframework.markers import database, cluster, sftp, aws, http

from benchpress import Benchpress

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook

@pytest.mark.parametrize('origin', ('SFTP Client',))
@pytest.mark.parametrize('destination', ('Trash', 'HTTP Client', 'Local FS', 'JDBC Producer', 'Kafka Producer', 'S3'))
@pytest.mark.parametrize('dataset', ('narrow', 'wide'))
@pytest.mark.parametrize('number_of_threads', (1,))
@pytest.mark.parametrize('batch_size', (1000,))
@pytest.mark.parametrize('destination_format', ('DELIMITED',))
@pytest.mark.parametrize('number_of_processors', (0, 4))
@database
@cluster('kafka')
@sftp
@aws('s3')
@http
def test_benchpress(sdc_builder, sdc_executor, benchmark_args, origin, destination,
                    dataset, number_of_threads, batch_size, destination_format, number_of_processors, database, cluster, sftp, aws, http_client):

    Benchpress(sdc_builder, sdc_executor, benchmark_args, origin, destination,
               dataset=dataset,
               number_of_threads=number_of_threads,
               batch_size=batch_size,
               destination_format=destination_format,
               number_of_processors=number_of_processors,
               database=database,
               kafka=cluster,
               sftp=sftp,
               s3=aws,
               http=http_client).rep()
