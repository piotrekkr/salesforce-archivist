import pytest
from simple_salesforce.api import Usage
from salesforce_archivist.salesforce import ApiUsage, Client, Salesforce


@pytest.mark.parametrize('used, total, percent', [(15, 100, 15.0), (999, 1000, 99.9), (97, 501, 19.36)])
def test_api_usage(used: int, total: int, percent: float):
    sf_usage = Usage(used=used, total=total)
    api_usage = ApiUsage(sf_usage)
    assert api_usage.used == used
    assert api_usage.total == total
    assert api_usage.percent == percent
