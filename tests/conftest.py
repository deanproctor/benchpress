import pytest

@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
    return hook
