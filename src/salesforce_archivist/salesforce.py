from __future__ import annotations

import csv
import glob
import os.path
import shutil
import threading
from math import ceil
from queue import Empty, Queue
from time import sleep
from typing import TYPE_CHECKING

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
from salesforce_archivist.document_link import ContentDocumentLink, ContentDocumentLinkList

if TYPE_CHECKING:
    from salesforce_archivist.archivist import ArchivistObject


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


class SalesforceApiClient:
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
        archivist_obj: ArchivistObject,
        client: SalesforceApiClient,
        dir_name_field: str | None = None,
        max_api_usage_percent: float | None = None,
    ):
        self._archivist_obj = archivist_obj
        self._client = client
        self._max_api_usage_percent = max_api_usage_percent
        self._dir_name_field = dir_name_field

    def _init_tmp_dir(self) -> str:
        tmp_dir = os.path.join(self._archivist_obj.data_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        for entry in os.scandir(tmp_dir):
            if entry.is_file():
                os.remove(entry.path)
        return tmp_dir

    def _get_content_document_list_query(self) -> str:
        select_list = ["LinkedEntityId", "ContentDocumentId"]
        if self._archivist_obj.dir_name_field is not None and self._archivist_obj.dir_name_field not in select_list:
            select_list.append(self._archivist_obj.dir_name_field)
        where_list = ["LinkedEntity.Type = '{obj_type}'".format(obj_type=self._archivist_obj.obj_type)]
        if self._archivist_obj.modified_date_lt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate < {date}".format(
                    date=self._archivist_obj.modified_date_lt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        if self._archivist_obj.modified_date_gt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate > {date}".format(
                    date=self._archivist_obj.modified_date_gt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        return "SELECT {fields} FROM ContentDocumentLink WHERE {where}".format(
            fields=", ".join(select_list), where=" AND ".join(where_list)
        )

    def download_content_document_link_list(
        self,
        document_link_list: ContentDocumentLinkList,
        max_records: int = 50000,
    ) -> None:
        tmp_dir = self._init_tmp_dir()
        query = self._get_content_document_list_query()
        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)

        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    link = ContentDocumentLink(
                        linked_entity_id=row[0],
                        content_document_id=row[1],
                        download_dir_name=row[2] if self._archivist_obj.dir_name_field is not None else None,
                    )
                    document_link_list.add_link(link)

    def load_document_link_list(self) -> ContentDocumentLinkList:
        document_link_list = ContentDocumentLinkList(
            data_dir=self._archivist_obj.data_dir, dir_name_field=self._archivist_obj.dir_name_field
        )
        if not os.path.exists(document_link_list.path):
            try:
                self.download_content_document_link_list(document_link_list=document_link_list)
            finally:
                document_link_list.save()
        return document_link_list

    def load_content_version_list(
        self,
        document_link_list: ContentDocumentLinkList,
        batch_size: int = 3000,
        progressbar: ProgressBar | None = None,
    ) -> ContentVersionList:
        content_version_list = ContentVersionList(data_dir=self._archivist_obj.data_dir)
        if not os.path.exists(content_version_list.path):
            try:
                doc_id_list = [link.content_document_id for link in document_link_list.get_links().values()]
                list_size = len(doc_id_list)
                all_batches = ceil(list_size / batch_size)

                for batch in range(1, all_batches + 1):
                    start = (batch - 1) * batch_size
                    end = start + batch_size
                    doc_id_batch = doc_id_list[start:end]
                    self.download_content_version_list(
                        document_ids=doc_id_batch,
                        content_version_list=content_version_list,
                    )
                    if progressbar is not None:
                        progressbar.update(batch)
            finally:
                content_version_list.save()
        if progressbar is not None:
            progressbar.update(len(document_link_list))
        return content_version_list

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

    def download_files(
        self,
        download_queue: ContentVersionDownloaderQueue,
        downloaded_versions_list: DownloadedContentVersionList,
        progressbar: ProgressBar | None = None,
    ) -> None:
        queue = download_queue.get_queue()
        try:
            threads = []
            downloader = ContentVersionDownloader(
                sf_client=self._client,
                downloaded_versions_list=downloaded_versions_list,
                max_api_usage_percent=self._max_api_usage_percent,
                progressbar=progressbar,
            )
            for i in range(3):
                thread = threading.Thread(
                    target=downloader.download_content_versions_in_queue,
                    kwargs={
                        "worker_num": i,
                        "queue": queue,
                    },
                    daemon=True,
                )
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()
        finally:
            downloaded_versions_list.save()


class ContentVersionDownloaderQueue:
    def __init__(
        self,
        document_link_list: ContentDocumentLinkList,
        content_version_list: ContentVersionList,
        archivist_obj: ArchivistObject,
    ):
        self._document_link_list = document_link_list
        self._content_version_list = content_version_list
        self._archivist_obj = archivist_obj
        self._queue: Queue | None = None

    def get_queue(self) -> Queue:
        if self._queue is None:
            self._queue = Queue()
            for link in self._document_link_list.get_links().values():
                for version in self._content_version_list.get_content_versions_for_link(link):
                    path = os.path.join(
                        self._archivist_obj.data_dir,
                        "files",
                        link.download_dir_name,
                        version.filename,
                    )
                    self._queue.put((version, path))
        return self._queue

    def __len__(self) -> int:
        return self.get_queue().qsize()


class ContentVersionDownloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        downloaded_versions_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
        progressbar: ProgressBar | None = None,
    ):
        self._client = sf_client
        self._downloaded_versions_list = downloaded_versions_list
        self._max_api_usage_percent = max_api_usage_percent
        self._progressbar = progressbar

    def download_content_version(self, version: ContentVersion, download_path: str) -> None:
        downloaded_version = self._downloaded_versions_list.get_version(version)
        # file exist under the path that we want to download into
        if os.path.exists(download_path):
            # if no version exist in downloaded list add a new downloaded version with this path
            if downloaded_version is None:
                downloaded_version = DownloadedContentVersion(
                    id=version.id,
                    document_id=version.document_id,
                    path=download_path,
                )
                self._downloaded_versions_list.add_version(downloaded_version)

        # version is on downloaded list and version points to existing file on disk
        elif downloaded_version is not None and os.path.exists(downloaded_version.path):
            # copy existing file if download path is different from already downloaded version path in the list
            if downloaded_version.path != download_path:
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                shutil.copy(downloaded_version.path, download_path)

        # download version using SF API and add to the list
        else:
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
            self._downloaded_versions_list.add_version(downloaded_version)

    def download_content_versions_in_queue(
        self, queue: Queue, worker_num: int | None = None, verbose: bool = False
    ) -> None:
        while True:
            try:
                queue_item: tuple[ContentVersion, str] = queue.get_nowait()
            except Empty:
                break

            try:
                version, download_path = queue_item
                self.download_content_version(version=version, download_path=download_path)
                if verbose:
                    click.echo(
                        "[W:{worker}][API usage: {usage:.2f}%] [OK] Downloaded content version {id} into {path}".format(
                            worker=worker_num,
                            usage=self._client.get_api_usage().percent,
                            id=version.id,
                            path=download_path,
                        )
                    )
                self.wait_if_api_usage_limit()
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
                if self._progressbar is not None:
                    self._progressbar.update(1)

    def wait_if_api_usage_limit(self, sleep_sec: int = 300) -> None:
        if self._max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= self._max_api_usage_percent:
                sleep(sleep_sec)
                usage = self._client.get_api_usage(refresh=True)
