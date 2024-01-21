import csv
import datetime
import glob
import os.path
import shutil
from queue import Empty, Queue
from time import sleep

import click
from click._termui_impl import ProgressBar
from requests import Response
from simple_salesforce import Salesforce as SimpleSFClient
from simple_salesforce.api import Usage

from salesforce_archivist.content_version import (
    ContentVersion,
    ContentVersionList,
    DownloadedContentVersion,
    DownloadedContentVersionList,
)
from salesforce_archivist.document_link import (
    ContentDocumentLink,
    ContentDocumentLinkList,
)


class ApiUsage:
    def __init__(self, usage: Usage):
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


class Client:
    def __init__(self, sf_client: SimpleSFClient):
        self._simple_sf_client = sf_client

    def bulk2(self, query: str, path: str, max_records: int) -> list[dict]:
        result: list[dict] = self._simple_sf_client.bulk2.Account.download(
            query=query, path=path, max_records=max_records
        )
        return result

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

    def get_api_usage(self, refresh: bool = False) -> ApiUsage:
        if refresh:
            self._simple_sf_client.limits()
        return ApiUsage(self._simple_sf_client.api_usage["api-usage"])


class Salesforce:
    def __init__(
        self,
        data_dir: str,
        client: Client,
        max_api_usage_percent: float | None = None,
    ):
        self._data_dir = data_dir
        self._client = client
        self._max_api_usage_percent = max_api_usage_percent

    def _init_tmp_dir(self) -> str:
        tmp_dir = os.path.join(self._data_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        for entry in os.scandir(os.path.join(self._data_dir, "tmp")):
            if entry.is_file():
                os.remove(entry.path)
        return tmp_dir

    @staticmethod
    def _get_content_document_list_query(
        obj_type: str,
        modified_date_lt: datetime.datetime | None = None,
        modified_date_gt: datetime.datetime | None = None,
        dir_name_field: str | None = None,
    ) -> str:
        select_list = ["LinkedEntityId", "ContentDocumentId"]
        if dir_name_field is not None and dir_name_field not in select_list:
            select_list.append(dir_name_field)
        where_list = ["LinkedEntity.Type = '{obj_type}'".format(obj_type=obj_type)]
        if modified_date_lt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate < {date}".format(
                    date=modified_date_lt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        if modified_date_gt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate > {date}".format(
                    date=modified_date_gt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        return "SELECT {fields} FROM ContentDocumentLink WHERE {where}".format(
            fields=", ".join(select_list), where=" AND ".join(where_list)
        )

    def download_content_document_link_list(
        self,
        document_link_list: ContentDocumentLinkList,
        obj_type: str,
        modified_date_lt: datetime.datetime | None = None,
        modified_date_gt: datetime.datetime | None = None,
        max_records: int = 50000,
        dir_name_field: str | None = None,
    ) -> None:
        tmp_dir = self._init_tmp_dir()
        query = self._get_content_document_list_query(
            obj_type=obj_type,
            modified_date_gt=modified_date_gt,
            modified_date_lt=modified_date_lt,
            dir_name_field=dir_name_field,
        )
        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)

        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    link = ContentDocumentLink(
                        linked_entity_id=row[0],
                        content_document_id=row[1],
                        download_dir_name=row[2] if dir_name_field is not None else None,
                    )
                    document_link_list.add_link(link)

    def download_content_version_list(
        self,
        document_ids: list[str],
        content_version_list: ContentVersionList,
        max_records: int = 50000,
    ) -> None:
        query = "SELECT Id, ContentDocumentId, Checksum, Title, FileExtension FROM ContentVersion WHERE ContentDocumentId IN ({id_list})".strip().format(
            id_list=",".join(["'{id}'".format(id=doc_id) for doc_id in document_ids])
        )
        tmp_dir = self._init_tmp_dir()
        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)
        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    version = ContentVersion(
                        id=row[0],
                        document_id=row[1],
                        checksum=row[2],
                        title=row[3],
                        extension=row[4],
                    )
                    content_version_list.add_version(version)

    def content_version_downloader(
        self,
        worker_num: int,
        queue: Queue,
        downloaded_versions_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
        progressbar: ProgressBar | None = None,
    ) -> None:
        while True:
            try:
                queue_item: tuple[ContentVersion, str] = queue.get_nowait()
            except Empty:
                break

            try:
                version, download_path = queue_item
                downloaded_version = downloaded_versions_list.get_version(version)
                if os.path.exists(download_path):
                    if downloaded_version is None:
                        downloaded_version = DownloadedContentVersion(
                            id=version.id,
                            document_id=version.document_id,
                            path=download_path,
                        )
                        downloaded_versions_list.add_version(downloaded_version)
                    click.echo(
                        "[W:{worker}] [NOTICE] Content version {id} already downloaded. Skipping".format(
                            id=version.id, worker=worker_num
                        )
                    )
                    continue

                if downloaded_version is not None and os.path.exists(downloaded_version.path):
                    if downloaded_version.path != download_path:
                        click.echo(
                            "[W:{worker}] [NOTICE] Copying already downloaded content version {id} from {src} to {dst}"
                            .format(
                                id=version.id,
                                src=downloaded_version.path,
                                dst=download_path,
                                worker=worker_num,
                            )
                        )
                        os.makedirs(os.path.dirname(download_path), exist_ok=True)
                        shutil.copy(downloaded_version.path, download_path)
                    continue

                result = self._client.download_content_version(version)
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                with open(download_path, "wb") as file:
                    for chunk in result.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)

                downloaded_version = DownloadedContentVersion(
                    id=version.id,
                    document_id=version.document_id,
                    path=download_path,
                )
                downloaded_versions_list.add_version(downloaded_version)

                click.echo(
                    "[W:{worker}][API usage: {usage:.2f}%] [OK] Downloaded content version {id} into {path}".format(
                        worker=worker_num,
                        id=version.id,
                        path=download_path,
                        usage=self._client.get_api_usage().percent,
                    )
                )
                self._wait_if_usage_limit_hit(max_api_usage_percent=max_api_usage_percent)
            except Exception as e:
                click.echo(
                    "[W:{worker}][API usage: {usage:.2f}%] [ERROR] Failed to download content version {id}: {error}"
                    .format(
                        id=queue_item[0].id,
                        error=e,
                        worker=worker_num,
                        usage=self._client.get_api_usage().percent,
                    )
                )
            finally:
                queue.task_done()
                if progressbar is not None:
                    progressbar.update(1)

    def _wait_if_usage_limit_hit(self, max_api_usage_percent: float | None = None) -> None:
        if max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= max_api_usage_percent:
                sleep(5 * 60)
                usage = self._client.get_api_usage(refresh=True)
