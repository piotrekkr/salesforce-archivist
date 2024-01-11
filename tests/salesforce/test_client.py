from requests import Response
from unittest.mock import Mock, call

from salesforce_archivist.content_version import ContentVersion
from salesforce_archivist.salesforce import Client


def test_bulk2():
    expected_result = [{"test": 1}]
    mock_sf = Mock()
    mock_sf.bulk2.Account.download.return_value = expected_result
    client = Client(sf_client=mock_sf)
    assert client.bulk2("query", "path", 1) == expected_result


def test_download_content_version():
    content_version = ContentVersion(
        id="VID", document_id="DID", extension="pdf", title="Title", checksum="MD5"
    )
    sf_base_url = "https://example.com"
    mock_sf = Mock()
    mock_sf.base_url = sf_base_url
    expected_result = Response()
    mock_sf._call_salesforce.return_value = expected_result
    client = Client(sf_client=mock_sf)

    assert client.download_content_version(version=content_version) == expected_result

    expected_calls = call._call_salesforce(
        url="{base}/sobjects/ContentVersion/{id}/VersionData".format(
            base=sf_base_url, id=content_version.id
        ),
        method="GET",
        headers={"Content-Type": "application/octet-stream"},
        stream=True,
    ).call_list()

    assert mock_sf.mock_calls == expected_calls
