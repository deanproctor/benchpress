import csv
import json
import psutil
import os
import sqlalchemy
import statistics
import string
import tempfile

from kafka.admin import KafkaAdminClient
from urllib.request import urlopen
from streamsets.sdk.utils import Version
from streamsets.testframework.utils import get_random_string

record_count = 50000000
benchmark_runs = 3
dataset_dir = 'resources'
dataset_suffix = '.00001.csv'
load_timeout = 86400
std_dev_threshold = 1

datasets = {'wide': {'file_pattern': 'cardtxn_50m', 'delimiter': ',', 'label': 'wide (74 cols, 525 bytes per row)'},
            'narrow': {'file_pattern': 'census_50m', 'delimiter': ',', 'label': 'narrow (13 cols, 160 bytes per row)'}}

sdc_builder = None
sdc_executor = None
dataset = None
number_of_threads = None
batch_size = None
destination_format = None
benchmark_args = None
database = None
kafka = None
kafka_topic = None

def run_test(builder, executor, origin_stage, destination_stage, my_dataset, threads, my_batch_size, my_destination_format, num_processors, my_benchmark_args, database_env=None, kafka_env=None):
    global sdc_builder, sdc_executor, origin, destination, dataset, number_of_threads, batch_size, destination_format, benchmark_args, record_count, database, kafka
    sdc_builder = builder
    sdc_executor = executor
    dataset = my_dataset
    number_of_threads = threads
    batch_size = my_batch_size
    destination_format = my_destination_format
    benchmark_args = my_benchmark_args
    database = database_env
    kafka = kafka_env
    record_count = benchmark_args.get('RECORD_COUNT') or record_count
    runs = benchmark_args.get('RUNS') or benchmark_runs

    origin, pipeline_builder = get_origin(origin_stage, number_of_threads)
    destination, pipeline_builder = get_destination(destination_stage, pipeline_builder)

    if num_processors == 4:
        stream_selector, pipeline_builder = get_stream_selector(pipeline_builder)
        expression_evaluator, pipeline_builder = get_expression_evaluator(pipeline_builder)
        field_type_converter, pipeline_builder = get_field_type_converter(pipeline_builder)
        schema_generator, pipeline_builder = get_schema_generator(pipeline_builder)
        trash, pipeline_builder = get_destination('Trash', pipeline_builder)
    
        origin >> stream_selector
        stream_selector >> trash
        stream_selector >> expression_evaluator >> field_type_converter >> schema_generator >> destination 

        stream_selector.condition = [{'outputLane': stream_selector.output_lanes[0],
                                      'predicate': '${record:attribute("sourceId") == "DOESNOTEXIST"}'},
                                     {'outputLane': stream_selector.output_lanes[1],
                                      'predicate': 'default'}]

    else: 
        origin >> destination

    pipeline = pipeline_builder.build().configure_for_environment(database,kafka)

    results = sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=runs)

    results['sdc_version'] = sdc_builder.version
    results['origin'] = origin_stage
    results['destination'] = destination_stage
    results['record_count'] = record_count
    results['threads'] = number_of_threads
    results['dataset'] = datasets[dataset]['label']
    results['batch_size'] = batch_size
    results['destination_data_format'] = destination_format
    results['processor_count'] = num_processors
    results['cpu_count'] = len(psutil.Process().cpu_affinity())
    results['memory_gb'] = round(psutil.virtual_memory().total / (1000 ** 3))
    try: 
        results['instance_type'] = urlopen('http://169.254.169.254/latest/meta-data/instance-type').read().decode('utf-8')
    except:
        results['instance_type'] = 'unknown'

    # Remove outliers
    if len(results['runs']) > 1:
        results['runs'] = [x for x in results['runs'] if -std_dev_threshold < (x - results['throughput_mean']) / results['throughput_std_dev'] < std_dev_threshold]
        results['throughput_mean'] = statistics.mean(results['runs'])

    with open("results/" + results['pipeline_id'] + ".json", "w") as file:
        json.dump(results, file)

    #cleanup
    if destination_stage == 'Kafka':
        admin_client = KafkaAdminClient(bootstrap_servers=kafka.kafka.brokers, client_id='admin')
        admin_client.delete_topics([kafka_topic])

def setup_origin_table():
    if create_table_if_not_exists(dataset):
        return

    directory, pipeline_builder = get_origin('Directory', 8)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 field_to_column_mapping=[],
                                 enclose_object_names = True,
                                 use_multi_row_operation = True,
                                 statement_parameter_limit = 32768,
                                 table_name=dataset)

    directory >> jdbc_producer

    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(record_count, timeout_sec=load_timeout)
    sdc_executor.stop_pipeline(pipeline)
    sdc_executor.remove_pipeline(pipeline)

def create_table_if_not_exists(table_name):
    if database.engine.dialect.has_table(database.engine, table_name):
        return True

    with open(dataset_dir + '/' + datasets[dataset]['file_pattern'] + dataset_suffix) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=datasets[dataset]['delimiter'])
        header = []
        header.append(next(csv_reader))
        metadata = sqlalchemy.MetaData()
        data_column = []
        for d in header[0]:
            if isinstance(header[0][d], int):
                data_column.append((d, sqlalchemy.Integer))
            elif isinstance(header[0][d], str):
                data_column.append((d, sqlalchemy.String))
            elif isinstance(header[0][d], bool):
                data_column.append((d, sqlalchemy.BOOLEAN))
            elif isinstance(header[0][d], float):
                data_column.append((d, sqlalchemy.FLOAT))
        table = sqlalchemy.Table(table_name, metadata, sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 *(sqlalchemy.Column(column_name, column_type(60))
                                   for column_name, column_type in data_column))
        table.create(database.engine)
    return False

def setup_origin_topic():
    if dataset in kafka.kafka.consumer().topics():
        return

    directory, pipeline_builder = get_origin('Directory', 8)
    kafka_producer = pipeline_builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                          library=kafka.kafka.standalone_stage_lib)
    kafka_producer.set_attributes(topic=dataset,
                                  data_format='DELIMITED',
                                  header_line='WITH_HEADER',
                                  delimiter_format='CUSTOM',
                                  delimiter_character=datasets[dataset]['delimiter'])

    directory >> kafka_producer
    
    pipeline = pipeline_builder.build().configure_for_environment(kafka)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(record_count, timeout_sec=load_timeout)
    sdc_executor.stop_pipeline(pipeline)
    sdc_executor.remove_pipeline(pipeline)


### Destinations

def get_destination(my_destination, pipeline_builder):
    destinations = {
        'JDBC Producer': get_jdbc_producer_destination,
        'Kafka Producer': get_kafka_producer_destination,
        'Local FS': get_localfs_destination,
        'Trash': get_trash_destination
    }
    stage = destinations.get(my_destination)
    return stage(pipeline_builder)

def get_trash_destination(pipeline_builder):
    trash = pipeline_builder.add_stage('Trash', type='destination')
    return trash, pipeline_builder

def get_localfs_destination(pipeline_builder):
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format=destination_format,
                            directory_template=tmp_directory,
                            files_prefix='sdc-${sdc:id()}', max_records_in_file=0)
    stop_stage = pipeline_builder.add_stop_event_stage('Shell')
    stop_stage.set_attributes(script='rm -rf ' + tmp_directory + ' &')

    return local_fs, pipeline_builder

def get_jdbc_producer_destination(pipeline_builder):
    table_name = get_random_string().lower()
    create_table_if_not_exists(table_name)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 field_to_column_mapping=[],
                                 enclose_object_names = True,
                                 use_multi_row_operation = True,
                                 statement_parameter_limit = 32768,
                                 table_name=table_name)
    query = 'drop table ' + table_name
    stop_stage = pipeline_builder.add_stop_event_stage('JDBC Query')
    if Version(sdc_builder.version) < Version('3.14.0'):
        stop_stage.set_attributes(sql_query=query)
    else:
        stop_stage.set_attributes(sql_queries=[query])

    return jdbc_producer, pipeline_builder

def get_kafka_producer_destination(pipeline_builder):
    global kafka_topic
    kafka_topic = get_random_string(string.ascii_letters, 10)
    kafka_producer = pipeline_builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                      library=kafka.kafka.standalone_stage_lib)
    kafka_producer.set_attributes(topic=kafka_topic,
                                  data_format=destination_format,
                                  header_line='WITH_HEADER',
                                  delimiter_format='CUSTOM',
                                  delimiter_character=datasets[dataset]['delimiter'])
    return kafka_producer, pipeline_builder


### Origins

def get_origin(my_origin, threads):
    origins = {
        'Directory': get_directory_origin,
        'JDBC Multitable Consumer': get_jdbc_multitable_origin,
        'Kafka Multitopic Consumer': get_kafka_multitopic_origin
    }
    stage = origins.get(my_origin, threads)
    return stage(threads)

def get_directory_origin(threads):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             delimiter_format_type='CUSTOM',
                             delimiter_character=datasets[dataset]['delimiter'],
                             file_name_pattern=datasets[dataset]['file_pattern'] + '*', file_name_pattern_mode='GLOB',
                             files_directory='/resources/' + dataset_dir,
                             number_of_threads=threads,
                             batch_size_in_recs=batch_size)
    return directory, pipeline_builder

def get_jdbc_multitable_origin(threads):
    setup_origin_table()
    pipeline_builder = sdc_builder.get_pipeline_builder()
    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer', type='origin')
    jdbc_multitable_consumer.set_attributes(table_configs=[dict(tablePattern=dataset,partitioningMode='BEST_EFFORT',maxNumActivePartitions=-1)],
                                            max_batch_size_in_records=batch_size,
                                            number_of_threads=threads,
                                            maximum_pool_size=threads)
    return jdbc_multitable_consumer, pipeline_builder

def get_kafka_multitopic_origin(threads):
    setup_origin_topic()
    pipeline_builder = sdc_builder.get_pipeline_builder()
    kafka_multitopic_consumer = pipeline_builder.add_stage('Kafka Multitopic Consumer',
                                                type='origin',
                                                library=kafka.kafka.standalone_stage_lib)
    kafka_multitopic_consumer.set_attributes(data_format='DELIMITED',
                              header_line='WITH_HEADER',
                              delimiter_format_type='CUSTOM',
                              delimiter_character=datasets[dataset]['delimiter'],
                              consumer_group=get_random_string(string.ascii_letters, 10),
                              auto_offset_reset='EARLIEST',
                              max_batch_size_in_records=batch_size,
                              number_of_threads=threads,
                              topic_list=[dataset])
    return kafka_multitopic_consumer, pipeline_builder


### Processors

def get_stream_selector(pipeline_builder):
    stream_selector = pipeline_builder.add_stage('Stream Selector')
    return stream_selector, pipeline_builder


def get_expression_evaluator(pipeline_builder):
    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [
        {'attributeToSet': 'title', 'headerAttributeExpression': '${pipeline:title()}'},
        {'attributeToSet': 'name', 'headerAttributeExpression': '${pipeline:name()}'},
        {'attributeToSet': 'version', 'headerAttributeExpression': '${pipeline:version()}'},
        {'attributeToSet': 'id', 'headerAttributeExpression': '${pipeline:id()}'},
    ]
    return expression_evaluator, pipeline_builder

def get_field_type_converter(pipeline_builder):
    field_type_converter_config = [
        {
            'fields': ['/id'],
            'targetType': 'LONG',
            'dataLocale': 'en,US'
        }
    ]
    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=field_type_converter_config)
    return field_type_converter, pipeline_builder

def get_schema_generator(pipeline_builder):
    schema_generator = pipeline_builder.add_stage('Schema Generator')
    schema_generator.schema_name = 'test_schema'
    schema_generator.enable_cache = True
    schema_generator.cache_key_expression = '${record:attribute("file")}'
    return schema_generator, pipeline_builder
