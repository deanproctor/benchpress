import json
import statistics
import csv

record_count = 50000000
dataset_dir = 'resources'
dataset_suffix = '.00001.csv'
load_timeout = 86400

datasets = {'cardtxn': {'file_pattern': 'cardtxn_50m', 'delimiter': ',', 'label': 'cardtxn (73 cols, 515 bytes per row)'},
            'census': {'file_pattern': 'census_50m', 'delimiter': ',', 'label': 'census (12 cols, 150 bytes per row)'}}

def save_results(results, sdc_version, origin, destination, threads, dataset):
    """ Persists benchmark results as a JSON file"""
    results['sdc_version'] = sdc_version
    results['origin'] = origin
    results['destination'] = destination
    results['record_count'] = record_count
    results['threads'] = threads
    results['dataset'] = datasets[dataset]['label']

    # Remove outliers
    results['runs'] = [x for x in results['runs'] if -2 < (x - results['throughput_mean']) / results['throughput_std_dev'] < 2]
    results['throughput_mean'] = statistics.mean(results['runs'])

    with open("results/" + results['pipeline_title'] + ".json", "w") as file:
        json.dump(results, file)

def load_table(database, sdc_builder, sdc_executor, dataset):
    engine = database.engine
    if engine.dialect.has_table(engine, dataset):
        return
    with open(dataset_dir + '/' + datasets[dataset]['file_pattern'] + dataset_suffix) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=datasets[dataset]['delimiter'])
        header = []
        header.append(next(csv_reader))
        database.insert_data(dataset, header)
    pipeline_builder = sdc_builder.get_pipeline_builder()
    directory = pipeline_builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='DELIMITED',
                             header_line='WITH_HEADER',
                             delimiter_format_type='CUSTOM',
                             delimiter_character=datasets[dataset]['delimiter'],
                             file_name_pattern=datasets[dataset]['file_pattern'] + '*', file_name_pattern_mode='GLOB',
                             files_directory='/resources/' + dataset_dir,
                             number_of_threads=8,
                             batch_size_in_recs=5000)
    jdbc_producer = pipeline_builder.add_stage('JDBC Producer')
    jdbc_producer.set_attributes(default_operation="INSERT",
                                 field_to_column_mapping=[],
                                 enclose_object_names = True,
                                 use_multi_row_operation = True,
                                 statement_parameter_limit = 32768,
                                 table_name=dataset)

    directory >> jdbc_producer

    populate_pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(populate_pipeline)
    sdc_executor.start_pipeline(populate_pipeline).wait_for_pipeline_output_records_count(record_count, timeout_sec=load_timeout)
    sdc_executor.stop_pipeline(populate_pipeline)
    sdc_executor.remove_pipeline(populate_pipeline)

