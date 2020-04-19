import csv
import json
import psutil
import os
import statistics
import string
import tempfile

from urllib.request import urlopen
from streamsets.testframework.utils import get_random_string

record_count = 50000000
dataset_dir = 'resources'
dataset_suffix = '.00001.csv'
load_timeout = 86400
std_dev_threshold = 1

datasets = {'cardtxn': {'file_pattern': 'cardtxn_50m', 'delimiter': ',', 'label': 'cardtxn (73 cols, 515 bytes per row)'},
            'census': {'file_pattern': 'census_50m', 'delimiter': ',', 'label': 'census (12 cols, 150 bytes per row)'}}

sdc_builder = None
sdc_executor = None
dataset = None
number_of_threads = None
batch_size = None
destination_format = None
benchmark_args = None
environment = None

def run_test(builder, executor, origin_stage, destination_stage, my_dataset, threads, my_batch_size, my_destination_format, my_benchmark_args, my_environment=None):
    global sdc_builder, sdc_executor, environment, origin, destination, dataset, number_of_threads, batch_size, destination_format, benchmark_args, environment
    sdc_builder = builder
    sdc_executor = executor
    dataset = my_dataset
    number_of_threads = threads
    batch_size = my_batch_size
    destination_format = my_destination_format
    benchmark_args = my_benchmark_args
    environment = my_environment

    origin, pipeline_builder = get_origin(origin_stage, number_of_threads)
    destination, pipeline_builder = get_destination(destination_stage, pipeline_builder)

    origin >> destination

    if(environment != None):
        pipeline = pipeline_builder.build().configure_for_environment(environment)
    else:
        pipeline = pipeline_builder.build()

    results = sdc_executor.benchmark_pipeline(pipeline, record_count=record_count, runs=benchmark_args['RUNS'])

    results['sdc_version'] = sdc_builder.version
    results['origin'] = origin_stage
    results['destination'] = destination_stage
    results['record_count'] = record_count
    results['threads'] = number_of_threads
    results['dataset'] = datasets[dataset]['label']
    results['batch_size'] = batch_size
    results['destination_data_format'] = destination_format
    results['cpu_count'] = len(psutil.Process().cpu_affinity())
    results['memory_gb'] = round(psutil.virtual_memory().total / (1000 ** 3))
    results['instance_type'] = urlopen('http://169.254.169.254/latest/meta-data/instance-type').read().decode('utf-8')

    # Remove outliers
    if len(results['runs']) > 1:
        results['runs'] = [x for x in results['runs'] if -std_dev_threshold < (x - results['throughput_mean']) / results['throughput_std_dev'] < std_dev_threshold]
        results['throughput_mean'] = statistics.mean(results['runs'])

    with open("results/" + results['pipeline_id'] + ".json", "w") as file:
        json.dump(results, file)

def setup_origin_table():
    if environment.engine.dialect.has_table(environment.engine, dataset):
        return
    with open(dataset_dir + '/' + datasets[dataset]['file_pattern'] + dataset_suffix) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=datasets[dataset]['delimiter'])
        header = []
        header.append(next(csv_reader))
        environment.insert_data(dataset, header)
    directory, pipeline_builder = get_origin('Directory', 8)
    jdbc_producer = get_destination('JDBC Producer', pipeline_builder)

    directory >> jdbc_producer

    pipeline = pipeline_builder.build().configure_for_environment(environment)
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(populate_pipeline).wait_for_pipeline_output_records_count(record_count, timeout_sec=load_timeout)
    sdc_executor.stop_pipeline(populate_pipeline)
    sdc_executor.remove_pipeline(populate_pipeline)

def get_destination(my_destination, pipeline_builder):
    destinations = {
        'JDBC Producer': get_jdbc_producer_destination,
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
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 field_to_column_mapping=[],
                                 enclose_object_names = True,
                                 use_multi_row_operation = True,
                                 statement_parameter_limit = 32768,
                                 table_name=dataset)
    return jdbc_producer, pipeline_builder


def get_origin(my_origin, threads):
    origins = {
        'Directory': get_directory_origin,
        'JDBC Multitable Consumer': get_jdbc_multitable_origin
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
