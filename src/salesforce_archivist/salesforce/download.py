from __future__ import annotations

import csv
import os
import shutil
import threading
from queue import Empty, Queue
from time import sleep
from typing import TYPE_CHECKING

import click

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList

if TYPE_CHECKING:
    from salesforce_archivist.archivist import ArchivistObject


class DownloadedContentVersion:
    def __init__(self, id: str, document_id: str, path: str):
        self._id = id
        self._document_id = document_id
        self._path = path

    @property
    def id(self) -> str:
        return self._id

    @property
    def document_id(self) -> str:
        return self._document_id

    @property
    def path(self) -> str:
        return self._path


class DownloadedContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, DownloadedContentVersion] = {}
        self._path = os.path.join(data_dir, "downloaded_versions.csv")
        if os.path.exists(self._path):
            self._load_data()

    def _load_data(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                version = DownloadedContentVersion(
                    id=row[0],
                    document_id=row[1],
                    path=row[2],
                )
                self.add_version(version)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Id", "ContentDocumentId", "Path on disk"])
            for version_id, version in self._data.items():
                writer.writerow([
                    version.id,
                    version.document_id,
                    version.path,
                ])

    def add_version(self, version: DownloadedContentVersion) -> None:
        self._data[version.id] = version

    def is_downloaded(self, content_version: ContentVersion) -> bool:
        return content_version.id in self._data

    def get_version(self, content_version: ContentVersion) -> DownloadedContentVersion | None:
        return self._data.get(content_version.id)


class DownloadQueue:
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


class Downloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        downloaded_versions_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
    ):
        self._client = sf_client
        self._downloaded_versions_list = downloaded_versions_list
        self._max_api_usage_percent = max_api_usage_percent

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

    def _print_download_msg(
        self, queue_size: int, worker_num: int, api_usage: float, msg: str, downloaded_size: int
    ) -> None:
        percent = downloaded_size / queue_size * 100
        item_padded = "{{:{width}d}}".format(width=len(str(queue_size))).format(downloaded_size)
        click.echo(
            "[💾{downloaded}/{total} {percent:6.2f}%] [🤖️{worker:2d}] [☁️{usage:6.2f}%] {msg}".format(
                downloaded=item_padded, percent=percent, total=queue_size, worker=worker_num, usage=api_usage, msg=msg
            )
        )

    def download_content_versions_in_queue(
        self, download_queue: Queue, download_count: int, worker_num: int, lock: threading.Lock, downloaded_queue: Queue
    ) -> None:
        while True:
            try:
                queue_item: tuple[ContentVersion, str] = download_queue.get_nowait()
            except Empty:
                break
            api_usage: float = 0.0
            version, download_path = queue_item
            msg = "[OK] Downloaded content version {id} into {path}".format(
                id=version.id,
                path=download_path,
            )
            try:
                self.download_content_version(version=version, download_path=download_path)
                api_usage = self._client.get_api_usage().percent
                self.wait_if_api_usage_limit(worker_num=worker_num)
            except Exception as e:
                msg = "[ERROR] Failed to download content version {id}: {error}".format(
                    id=queue_item[0].id,
                    error=e,
                )
            finally:
                with lock:
                    downloaded_queue.put(queue_item)
                    self._print_download_msg(
                        queue_size=download_count,
                        worker_num=worker_num,
                        api_usage=api_usage,
                        msg=msg,
                        downloaded_size=downloaded_queue.qsize(),
                    )
                download_queue.task_done()

    def wait_if_api_usage_limit(self, worker_num: int, sleep_sec: int = 300, verbose: bool = False) -> None:
        if self._max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= self._max_api_usage_percent:
                if verbose:
                    click.echo(
                        "[W:{worker}][API usage: {usage:.2f}%] [NOTICE] Waiting for API limit to drop.".format(
                            worker=worker_num, usage=usage.percent
                        )
                    )
                sleep(sleep_sec)
                usage = self._client.get_api_usage(refresh=True)
