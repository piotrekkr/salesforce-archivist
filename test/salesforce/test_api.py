from unittest.mock import Mock, call

import pytest
from requests import Response
from simple_salesforce.api import Usage

from salesforce_archivist.salesforce.api import ApiUsage, SalesforceApiClient
from salesforce_archivist.salesforce.content_version import ContentVersion


@pytest.mark.parametrize("used, total, percent", [(15, 100, 15.0), (999, 1000, 99.9), (97, 501, 19.36)])
def test_api_usage(used: int, total: int, percent: float):
    sf_usage = Usage(used=used, total=total)
    api_usage = ApiUsage(sf_usage)
    assert api_usage.used == used
    assert api_usage.total == total
    assert api_usage.percent == percent


def test_bulk2():
    expected_result = [{"test": 1}]
    mock_sf = Mock()
    mock_sf.bulk2.Account.download.return_value = expected_result
    client = SalesforceApiClient(sf_client=mock_sf)
    assert client.bulk2("query", "path", 1) == expected_result


def test_download_content_version():
    content_version = ContentVersion(
        id="VID", document_id="DID", extension="pdf", title="Title", checksum="MD5", version_number=1
    )
    sf_base_url = "https://example.com"
    mock_sf = Mock()
    mock_sf.base_url = sf_base_url
    expected_result = Response()
    mock_sf._call_salesforce.return_value = expected_result
    client = SalesforceApiClient(sf_client=mock_sf)

    assert client.download_content_version(version=content_version) == expected_result

    expected_calls = call._call_salesforce(
        url="{base}/sobjects/ContentVersion/{id}/VersionData".format(base=sf_base_url, id=content_version.id),
        method="GET",
        headers={"Content-Type": "application/octet-stream"},
        stream=True,
    ).call_list()

    assert mock_sf.mock_calls == expected_calls


@pytest.mark.parametrize(
    "refresh, api_usage, should_call_limits, expected_used, expected_total",
    [
        [False, {"api-usage": Usage(used=1, total=10)}, False, 1, 10],
        [True, {"api-usage": Usage(used=1, total=10)}, True, 2, 20],
        [False, {}, True, 2, 20],
    ],
)
def test_get_api_usage(
    refresh: bool, api_usage: dict[str, Usage], should_call_limits: bool, expected_used: int, expected_total
):
    mock_sf = Mock()
    mock_sf.api_usage = api_usage
    if should_call_limits:

        def set_limits():
            mock_sf.api_usage = {"api-usage": Usage(used=expected_used, total=expected_total)}

        mock_sf.limits.side_effect = set_limits
    client = SalesforceApiClient(sf_client=mock_sf)
    result = client.get_api_usage(refresh=refresh)
    assert isinstance(result, ApiUsage)
    assert (expected_used, expected_total) == (result.used, result.total)
