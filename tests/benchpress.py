import csv
import json
import os
import statistics
import string
import tempfile

from datetime import datetime
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from kafka.admin import KafkaAdminClient, NewTopic

import psutil
import sqlalchemy

from streamsets.sdk.utils import Version
from streamsets.testframework.utils import get_random_string
from pretenders.common.constants import FOREVER

# pylint: disable=pointless-statement, too-many-instance-attributes

DEFAULT_RECORD_COUNT = 50000000
DEFAULT_BENCHMARK_RUNS = 3
DEFAULT_NUMBER_OF_THREADS = 1
DEFAULT_NUMBER_OF_PROCESSORS = 0
DEFAULT_BATCH_SIZE = 1000
DEFAULT_DATASET = 'narrow'
DEFAULT_DESTINATION_FORMAT = 'DELIMITED'
DATASET_DIR = 'resources'
DATASET_SUFFIX = '.00001.csv'
LOAD_TIMEOUT = 86400
MAX_CONCURRENCY = 8
STD_DEV_THRESHOLD = 1

DATASETS = {'wide': {'file_pattern': 'cardtxn_50m',
                     'delimiter': ',',
                     'label': 'wide (74 cols, 1450 bytes per row)'},
            'narrow': {'file_pattern': 'census_50m',
                       'delimiter': ',',
                       'label': 'narrow (13 cols, 275 bytes per row)'}}

class Benchpress():
    """Class that encapsulates a Benchpress instance."""
    def __init__(self, sdc_builder, sdc_executor, benchmark_args, origin, destination, **kwargs):
        self.sdc_builder = sdc_builder
        self.sdc_executor = sdc_executor
        self.record_count = benchmark_args.get('RECORD_COUNT', DEFAULT_RECORD_COUNT)
        self.runs = benchmark_args.get('RUNS', DEFAULT_BENCHMARK_RUNS)
        self.origin = origin
        self.destination = destination
        self.dataset = kwargs.get('dataset', DEFAULT_DATASET)
        self.number_of_threads = kwargs.get('number_of_threads', DEFAULT_NUMBER_OF_THREADS)
        self.number_of_processors = kwargs.get('number_of_processors', DEFAULT_NUMBER_OF_PROCESSORS)
        self.batch_size = kwargs.get('batch_size', DEFAULT_BATCH_SIZE)
        self.destination_format = kwargs.get('destination_format', DEFAULT_DESTINATION_FORMAT)

        self.environments = {}
        self.environments['database'] = kwargs.get('database')
        self.environments['kafka'] = kwargs.get('kafka')
        self.environments['sftp'] = kwargs.get('sftp')
        self.environments['s3'] = kwargs.get('s3')
        self.http = kwargs.get('http')

        self.origin_system = None
        self.destination_system = None

        self.destination_table = None
        self.destination_kafka_topic = None
        self.http_mock = None

    def rep(self):
        """Builds and runs the pipeline for the current parameter permutation."""
        origin, pipeline_builder = self._get_origin(self.origin)
        destination, pipeline_builder = self._get_destination(self.destination, pipeline_builder)

        if self.number_of_processors == 4:
            stream_selector, pipeline_builder = self._get_stream_selector(pipeline_builder)
            expression_evaluator, pipeline_builder = self._get_expression_evaluator(pipeline_builder)
            field_type_converter, pipeline_builder = self._get_field_type_converter(pipeline_builder)
            schema_generator, pipeline_builder = self._get_schema_generator(pipeline_builder)
            trash, pipeline_builder = self._get_destination('Trash', pipeline_builder)

            origin >> stream_selector
            stream_selector >> trash
            stream_selector >> expression_evaluator >> field_type_converter >> schema_generator >> destination

            stream_selector.condition = [{'outputLane': stream_selector.output_lanes[0],
                                          'predicate': '${record:attribute("sourceId") == "DOESNOTEXIST"}'},
                                         {'outputLane': stream_selector.output_lanes[1],
                                          'predicate': 'default'}]
        else:
            origin >> destination

        for environment in self.environments.values():
            if environment is not None:
                pipeline = pipeline_builder.build().configure_for_environment(environment)

        results = self.sdc_executor.benchmark_pipeline(pipeline,
                                                       record_count=self.record_count,
                                                       runs=self.runs)

        results['generated_date'] = str(datetime.now())
        results['sdc_version'] = self.sdc_builder.version
        results['origin'] = self.origin
        results['destination'] = self.destination
        results['record_count'] = self.record_count
        results['threads'] = self.number_of_threads
        results['dataset'] = DATASETS[self.dataset]['label']
        results['batch_size'] = self.batch_size
        results['destination_data_format'] = self.destination_format
        results['processor_count'] = self.number_of_processors
        results['cpu_count'] = len(psutil.Process().cpu_affinity())
        results['memory_gb'] = round(psutil.virtual_memory().total / (1000 ** 3))
        try:
            results['instance_type'] = urlopen('http://169.254.169.254/latest/meta-data/instance-type').read().decode('utf-8')
        except (HTTPError, URLError):
            results['instance_type'] = 'unknown'

        results['origin_system'] = self.origin_system
        results['destination_system'] = self.destination_system

        # Remove outliers
        if len(results['runs']) > 1:
            results['runs'] = [x for x in results['runs'] if -STD_DEV_THRESHOLD < (x - results['throughput_mean']) / results['throughput_std_dev'] < STD_DEV_THRESHOLD]
            results['throughput_mean'] = statistics.mean(results['runs'])

        with open(f"results/{results['pipeline_id']}.json", "w") as file:
            json.dump(results, file)

        # Cleanup
        if self.destination == 'Kafka Producer':
            admin_client = KafkaAdminClient(bootstrap_servers=self.environments['kafka'].kafka.brokers, request_timeout_ms=180000)
            admin_client.delete_topics([self.destination_kafka_topic])

        if self.destination == 'JDBC Producer':
            self.destination_table.drop(self.environments['database'].engine)

        if self.origin == 'HTTP Client':
            self.http_mock.delete_mock()

    def _setup_origin_table(self):
        """Creates and populates an origin table for the current dataset, if it doesn't already exist."""
        if self._create_table_if_not_exists(self.dataset):
            return

        directory, pipeline_builder = self._directory_origin(MAX_CONCURRENCY)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
        jdbc_producer.set_attributes(default_operation="INSERT",
                                     field_to_column_mapping=[],
                                     enclose_object_names=True,
                                     use_multi_row_operation=True,
                                     statement_parameter_limit=32768,
                                     table_name=self.dataset)

        directory >> jdbc_producer

        pipeline = pipeline_builder.build().configure_for_environment(self.environments['database'])
        self.sdc_executor.add_pipeline(pipeline)
        self.sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(self.record_count, timeout_sec=LOAD_TIMEOUT)
        self.sdc_executor.stop_pipeline(pipeline)
        self.sdc_executor.remove_pipeline(pipeline)

    def _create_table_if_not_exists(self, table_name):
        """Creates the specified table if it does not already exist."""
        if self.environments['database'].engine.dialect.has_table(self.environments['database'].engine, table_name):
            return True

        with open(f"{DATASET_DIR}/{DATASETS[self.dataset]['file_pattern']}{DATASET_SUFFIX}") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=DATASETS[self.dataset]['delimiter'])
            header = []
            header.append(next(csv_reader))
            metadata = sqlalchemy.MetaData()
            data_column = []
            for col in header[0]:
                if isinstance(header[0][col], int):
                    data_column.append((col, sqlalchemy.Integer))
                elif isinstance(header[0][col], str):
                    data_column.append((col, sqlalchemy.String))
                elif isinstance(header[0][col], bool):
                    data_column.append((col, sqlalchemy.BOOLEAN))
                elif isinstance(header[0][col], float):
                    data_column.append((col, sqlalchemy.FLOAT))
            table = sqlalchemy.Table(table_name,
                                     metadata,
                                     sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                     *(sqlalchemy.Column(column_name, column_type(60))
                                       for column_name, column_type in data_column))
            table.create(self.environments['database'].engine)
            self.destination_table = table
        return False

    def _setup_origin_topic(self):
        """Creates and populates an origin topic for the current dataset, if it doesn't already exist."""
        if self._create_topic_if_not_exists(self.dataset):
            return

        directory, pipeline_builder = self._directory_origin(MAX_CONCURRENCY)
        kafka_stage_name = 'com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget'
        kafka_producer = pipeline_builder.add_stage(name=kafka_stage_name,
                                                    library=self.environments['kafka'].kafka.standalone_stage_lib)
        kafka_producer.set_attributes(topic=self.dataset,
                                      data_format='DELIMITED',
                                      header_line='WITH_HEADER',
                                      delimiter_format='CUSTOM',
                                      delimiter_character=DATASETS[self.dataset]['delimiter'])

        directory >> kafka_producer

        pipeline = pipeline_builder.build().configure_for_environment(self.environments['kafka'])
        self.sdc_executor.add_pipeline(pipeline)
        self.sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(self.record_count, timeout_sec=LOAD_TIMEOUT)
        self.sdc_executor.stop_pipeline(pipeline)
        self.sdc_executor.remove_pipeline(pipeline)

    def _create_topic_if_not_exists(self, topic):
        """Creates the specified topic if it does not already exist."""
        if topic in self.environments['kafka'].kafka.consumer().topics():
            return True

        new_topic = NewTopic(name=topic, num_partitions=MAX_CONCURRENCY*2, replication_factor=1)
        admin_client = KafkaAdminClient(bootstrap_servers=self.environments['kafka'].kafka.brokers,
                                        request_timeout_ms=180000)
        admin_client.create_topics(new_topics=[new_topic], timeout_ms=180000)
        return False

    def _setup_http_mock(self):
        """Creates a mock HTTP server."""
        if self.http_mock is not None:
            return

        with open(f"{DATASET_DIR}/{DATASETS[self.dataset]['file_pattern']}{DATASET_SUFFIX}") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=DATASETS[self.dataset]['delimiter'])
            http_data = [next(csv_reader) for x in range(self.batch_size)]

        http_mock = self.http.mock()
        http_mock.when(f'GET /{self.dataset}').reply(json.dumps(http_data), times=FOREVER)
        self.http_mock = http_mock

    ### Destinations

    def _get_destination(self, destination, pipeline_builder):
        """Returns the appropriate destination stage based on the stage name."""
        destinations = {
            'JDBC Producer': self._jdbc_producer_destination,
            'Kafka Producer': self._kafka_producer_destination,
            'Local FS': self._localfs_destination,
            'S3': self._s3_destination,
            'SFTP Client': self._sftp_client_destination,
            'Trash': self._trash_destination
        }
        stage = destinations.get(destination)
        return stage(pipeline_builder)

    @staticmethod
    def _trash_destination(pipeline_builder):
        """Returns an instance of the Trash destination."""
        trash = pipeline_builder.add_stage('Trash', type='destination')
        return trash, pipeline_builder

    def _localfs_destination(self, pipeline_builder):
        """Returns an instance of the Local FS destination."""
        tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
        local_fs = pipeline_builder.add_stage('Local FS', type='destination')
        local_fs.set_attributes(data_format=self.destination_format,
                                directory_template=tmp_directory,
                                files_prefix='sdc-${sdc:id()}', max_records_in_file=0)
        stop_stage = pipeline_builder.add_stop_event_stage('Shell')
        stop_stage.set_attributes(script=f'rm -rf {tmp_directory} &')

        return local_fs, pipeline_builder

    def _jdbc_producer_destination(self, pipeline_builder):
        """Returns an instance of the JDBC Producer destination."""
        self.destination_system = self.environments['database'].engine.dialect.name
        self.destination_format = None

        table_name = get_random_string().lower()
        self._create_table_if_not_exists(table_name)
        jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
        jdbc_producer.set_attributes(default_operation="INSERT",
                                     field_to_column_mapping=[],
                                     enclose_object_names=True,
                                     use_multi_row_operation=True,
                                     statement_parameter_limit=32768,
                                     table_name=table_name)
        query = f'TRUNCATE TABLE {table_name}'
        stop_stage = pipeline_builder.add_stop_event_stage('JDBC Query')
        if Version(self.sdc_builder.version) < Version('3.14.0'):
            stop_stage.set_attributes(sql_query=query)
        else:
            stop_stage.set_attributes(sql_queries=[query])
        return jdbc_producer, pipeline_builder

    def _kafka_producer_destination(self, pipeline_builder):
        """Returns an instance of the Kafka Producer destination."""
        self.destination_kafka_topic = get_random_string(string.ascii_letters, 10)
        self._create_topic_if_not_exists(self.destination_kafka_topic)
        kafka_stage_name = 'com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget'
        kafka_producer = pipeline_builder.add_stage(name=kafka_stage_name,
                                                    library=self.environments['kafka'].kafka.standalone_stage_lib,
                                                    type='destination')
        kafka_producer.set_attributes(topic=self.destination_kafka_topic,
                                      data_format=self.destination_format,
                                      header_line='WITH_HEADER',
                                      delimiter_format='CUSTOM',
                                      delimiter_character=DATASETS[self.dataset]['delimiter'])
        return kafka_producer, pipeline_builder

    def _s3_destination(self, pipeline_builder):
        """Returns an instance of the S3 Destination."""
        s3_destination = pipeline_builder.add_stage('Amazon S3', type='destination')
        s3_destination.set_attributes(bucket=self.environments['s3'].s3_bucket_name,
                                      common_prefix='destination_data',
                                      partition_prefix=get_random_string(string.ascii_letters, 10),
                                      data_format=self.destination_format,
                                      header_line='WITH_HEADER',
                                      delimiter_format='CUSTOM',
                                      delimiter_character=DATASETS[self.dataset]['delimiter'])
        return s3_destination, pipeline_builder

    @staticmethod
    def _sftp_client_destination(pipeline_builder):
        """Returns an instance of the SFTP Client destination."""
        sftp_stage_name = 'com_streamsets_pipeline_stage_destination_remote_RemoteUploadDTarget'
        sftp_client = pipeline_builder.add_stage(name=sftp_stage_name, type='destination')
        sftp_client.file_name_expression = '${uuid:uuid()}'
        return sftp_client, pipeline_builder


    ### Origins

    def _get_origin(self, origin):
        """Returns the appropriate origin stage based on the stage name."""
        origins = {
            'Directory': self._directory_origin,
            'HTTP Client': self._http_client_origin,
            'JDBC Multitable Consumer': self._jdbc_multitable_origin,
            'JDBC Query Consumer': self._jdbc_query_origin,
            'Kafka Multitopic Consumer': self._kafka_multitopic_origin,
            'S3': self._s3_origin,
            'SFTP Client': self._sftp_client_origin
        }
        stage = origins.get(origin)
        return stage()

    def _directory_origin(self, threads=None):
        """Returns an instance of a Directory origin."""
        if threads is None:
            threads = self.number_of_threads
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        directory = pipeline_builder.add_stage('Directory', type='origin')
        directory.set_attributes(data_format='DELIMITED',
                                 header_line='WITH_HEADER',
                                 delimiter_format_type='CUSTOM',
                                 delimiter_character=DATASETS[self.dataset]['delimiter'],
                                 file_name_pattern=f"{DATASETS[self.dataset]['file_pattern']}*",
                                 file_name_pattern_mode='GLOB',
                                 files_directory=f'/resources/{DATASET_DIR}',
                                 number_of_threads=threads,
                                 batch_size_in_recs=self.batch_size)
        return directory, pipeline_builder

    def _http_client_origin(self):
        """Returns an instance of an HTTP Client origin."""
        self._setup_http_mock()
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        http_client = pipeline_builder.add_stage('HTTP Client', type='origin')
        http_client.resource_url = f'{self.http_mock.pretend_url}/{self.dataset}'
        http_client.json_content = 'ARRAY_OBJECTS'
        return http_client, pipeline_builder

    def _jdbc_multitable_origin(self):
        """Returns an instance of a JDBC Multitable origin."""
        self.origin_system = self.environments['database'].engine.dialect.name
        table_configs = [dict(tablePattern=self.dataset,
                              partitioningMode='BEST_EFFORT',
                              maxNumActivePartitions=-1)]

        self._setup_origin_table()

        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer', type='origin')
        jdbc_multitable_consumer.set_attributes(table_configs=table_configs,
                                                max_batch_size_in_records=self.batch_size,
                                                number_of_threads=self.number_of_threads,
                                                maximum_pool_size=self.number_of_threads)
        return jdbc_multitable_consumer, pipeline_builder

    def _jdbc_query_origin(self):
        """Returns an instance of a JDBC Query origin."""
        self.origin_system = self.environments['database'].engine.dialect.name
        self._setup_origin_table()
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer', type='origin')
        jdbc_query_consumer.set_attributes(incremental_mode=False,
                                           sql_query=f'SELECT * FROM {self.dataset}')
        return jdbc_query_consumer, pipeline_builder

    def _kafka_multitopic_origin(self):
        """Returns an instance of a Kafka Multitopic origin."""
        self._setup_origin_topic()
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        kafka_multitopic_consumer = pipeline_builder.add_stage('Kafka Multitopic Consumer',
                                                               type='origin',
                                                               library=self.environments['kafka'].kafka.standalone_stage_lib)
        kafka_multitopic_consumer.set_attributes(data_format='DELIMITED',
                                                 header_line='WITH_HEADER',
                                                 delimiter_format_type='CUSTOM',
                                                 delimiter_character=DATASETS[self.dataset]['delimiter'],
                                                 consumer_group=get_random_string(string.ascii_letters, 10),
                                                 auto_offset_reset='EARLIEST',
                                                 max_batch_size_in_records=self.batch_size,
                                                 number_of_threads=self.number_of_threads,
                                                 topic_list=[self.dataset])
        return kafka_multitopic_consumer, pipeline_builder

    def _s3_origin(self):
        """Returns an instance of an AWS S3 origin."""
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        s3_origin = pipeline_builder.add_stage('Amazon S3', type='origin')
        s3_origin.set_attributes(bucket=self.environments['s3'].s3_bucket_name,
                                 common_prefix='origin_data',
                                 prefix_pattern=f"{DATASETS[self.dataset]['file_pattern']}*",
                                 data_format='DELIMITED',
                                 header_line='WITH_HEADER',
                                 delimiter_format_type='CUSTOM',
                                 delimiter_character=DATASETS[self.dataset]['delimiter'],
                                 number_of_threads=self.number_of_threads,
                                 max_batch_size_in_records=self.batch_size)
        return s3_origin, pipeline_builder

    def _sftp_client_origin(self):
        """Returns an instance of an SFTP Client origin."""
        sftp_stage_name = 'com_streamsets_pipeline_stage_origin_remote_RemoteDownloadDSource'
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        sftp_client = pipeline_builder.add_stage(name=sftp_stage_name,
                                                 type='origin')
        sftp_client.set_attributes(data_format='DELIMITED',
                                   file_name_pattern=f"{DATASETS[self.dataset]['file_pattern']}*",
                                   delimiter_character=DATASETS[self.dataset]['delimiter'],
                                   header_line='WITH_HEADER')
        return sftp_client, pipeline_builder


    ### Processors

    @staticmethod
    def _get_stream_selector(pipeline_builder):
        """Returns an unconfigured instance of a Stream Selector processor."""
        stream_selector = pipeline_builder.add_stage('Stream Selector')
        return stream_selector, pipeline_builder

    @staticmethod
    def _get_expression_evaluator(pipeline_builder):
        """Returns an instance of an Expression Evaluator processor."""
        expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
        expression_evaluator.header_attribute_expressions = [
            {'attributeToSet': 'title', 'headerAttributeExpression': '${pipeline:title()}'},
            {'attributeToSet': 'name', 'headerAttributeExpression': '${pipeline:name()}'},
            {'attributeToSet': 'version', 'headerAttributeExpression': '${pipeline:version()}'},
            {'attributeToSet': 'id', 'headerAttributeExpression': '${pipeline:id()}'},
        ]
        return expression_evaluator, pipeline_builder

    @staticmethod
    def _get_field_type_converter(pipeline_builder):
        """Returns an instance of a Field Type Converter processor."""
        converter_config = [
            {
                'fields': ['/id'],
                'targetType': 'LONG',
                'dataLocale': 'en,US'
            }
        ]
        field_type_converter = pipeline_builder.add_stage('Field Type Converter')
        field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                            field_type_converter_configs=converter_config)
        return field_type_converter, pipeline_builder

    @staticmethod
    def _get_schema_generator(pipeline_builder):
        """Returns an instance of a Schema Generator processor."""
        schema_generator = pipeline_builder.add_stage('Schema Generator')
        schema_generator.schema_name = 'test_schema'
        schema_generator.enable_cache = True
        schema_generator.cache_key_expression = '${record:attribute("file")}'
        return schema_generator, pipeline_builder
