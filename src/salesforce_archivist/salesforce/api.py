from requests import Response
from simple_salesforce import Salesforce as SimpleSFClient
from simple_salesforce.api import Usage
from simple_salesforce.bulk2 import QueryResult
from simple_salesforce.util import PerAppUsage

from salesforce_archivist.salesforce.attachment import Attachment
from salesforce_archivist.salesforce.content_version import ContentVersion


class ApiUsage:
    def __init__(self, usage: Usage | PerAppUsage):
        self._used: int = usage.used
        self._total: int = usage.total

    @property
    def used(self) -> int:
        return self._used

    @property
    def total(self) -> int:
        return self._total

    @property
    def percent(self) -> float:
        return round(self.used / self.total * 100, 2) if self.total > 0 else 0.0


class SalesforceApiClient:
    def __init__(self, sf_client: SimpleSFClient):
        self._simple_sf_client = sf_client

    def bulk2(self, query: str, path: str, max_records: int) -> list[QueryResult]:
        return self._simple_sf_client.bulk2.Account.download(query=query, path=path, max_records=max_records)  # type: ignore[union-attr]

    def download_content_version(self, version: ContentVersion) -> Response:
        result: Response = self._simple_sf_client._call_salesforce(
            url="{base}/sobjects/ContentVersion/{id}/VersionData".format(
                base=self._simple_sf_client.base_url, id=version.id
            ),
            method="GET",
            headers={"Content-Type": "application/octet-stream"},
            stream=True,
        )
        return result

    def download_attachment(self, attachment: Attachment) -> Response:
        result: Response = self._simple_sf_client._call_salesforce(
            url="{base}/sobjects/Attachment/{id}/body".format(base=self._simple_sf_client.base_url, id=attachment.id),
            method="GET",
            headers={"Content-Type": "application/octet-stream"},
            stream=True,
        )
        return result

    def get_api_usage(self, refresh: bool = False) -> ApiUsage:
        if refresh or self._simple_sf_client.api_usage.get("api-usage") is None:
            self._simple_sf_client.limits()
        return ApiUsage(self._simple_sf_client.api_usage["api-usage"])
