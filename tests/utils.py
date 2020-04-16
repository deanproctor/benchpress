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

