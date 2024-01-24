from __future__ import annotations

import os.path
import shutil
from queue import Empty, Queue
from time import sleep
from typing import TYPE_CHECKING

import click

from salesforce_archivist.salesforce.document_link import ContentDocumentLink, ContentDocumentLinkList

if TYPE_CHECKING:
    from salesforce_archivist.salesforce.api import SalesforceApiClient
    from salesforce_archivist.archivist import ArchivistObject


import csv
import re
from typing import Any


class ContentVersion:
    def __init__(self, id: str, document_id: str, title: str, extension: str, checksum: str):
        self._id = id
        self._document_id = document_id
        self._title = title
        self._extension = extension
        self._checksum = checksum

    @property
    def id(self) -> str:
        return self._id

    @property
    def document_id(self) -> str:
        return self._document_id

    @property
    def title(self) -> str:
        return self._title

    @property
    def extension(self) -> str:
        return self._extension

    @property
    def checksum(self) -> str:
        return self._checksum

    @property
    def filename(self) -> str:
        return "{id}_{title}.{extension}".format(
            id=self.id,
            title=re.sub(r'[/\\?%*:|"<>]', "-", self.title),
            extension=self.extension,
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            self.id,
            self.document_id,
            self.title,
            self.extension,
            self.checksum,
        ) == (other.id, other.document_id, other.title, other.extension, other.checksum)


class ContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ContentVersion] = {}
        self._path = os.path.join(data_dir, "content_versions.csv")
        self._doc_versions_map: dict[str, set[str]] = {}
        if os.path.exists(self._path):
            self._load_data()

    def _load_data(self) -> None:
        with open(self._path) as file:
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
                self.add_version(version)

    def save(self) -> None:
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Id", "ContentDocumentId", "Checksum", "Title", "Extension"])
            for version_id, version in self._data.items():
                writer.writerow([
                    version.id,
                    version.document_id,
                    version.checksum,
                    version.title,
                    version.extension,
                ])

    def add_version(self, version: ContentVersion) -> None:
        if version.document_id not in self._doc_versions_map:
            self._doc_versions_map[version.document_id] = set()
        self._doc_versions_map[version.document_id].add(version.id)
        self._data[version.id] = version

    def get_content_versions_for_link(self, link: ContentDocumentLink) -> list[ContentVersion]:
        versions = []
        if link.content_document_id in self._doc_versions_map:
            for version_id in self._doc_versions_map[link.content_document_id]:
                versions.append(self._data[version_id])
        return versions

    @property
    def path(self) -> str:
        return self._path


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


class ValidatedContentVersion:
    def __init__(self, path: str, checksum: str):
        self._path = path
        self._checksum = checksum

    @property
    def path(self) -> str:
        return self._path

    @property
    def checksum(self) -> str:
        return self._checksum


class ValidatedContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ValidatedContentVersion] = {}
        self._path = os.path.join(data_dir, "validated_versions.csv")
        if os.path.exists(self._path):
            self._load_data()

    def _load_data(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                version = ValidatedContentVersion(checksum=row[0], path=row[1])
                self.add_version(version)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Checksum", "Path"])
            for version_id, version in self._data.items():
                writer.writerow([
                    version.checksum,
                    version.path,
                ])

    def add_version(self, version: ValidatedContentVersion) -> None:
        self._data[version.path] = version

    def is_validated(self, path: str) -> bool:
        return path in self._data

    def get_version(self, path: str) -> ValidatedContentVersion | None:
        return self._data.get(path)


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

    def download_content_versions_in_queue(
        self, queue: Queue, queue_size: int, worker_num: int, verbose: bool = True
    ) -> None:
        while True:
            try:
                queue_item: tuple[ContentVersion, str] = queue.get_nowait()
            except Empty:
                break
            item = queue_size - queue.qsize()
            api_usage: float | None = None
            try:
                version, download_path = queue_item
                self.download_content_version(version=version, download_path=download_path)
                api_usage = self._client.get_api_usage().percent
                if verbose:
                    click.echo(
                        "[{item} of {total}][W:{worker}][API usage: {usage:.2f}%] [OK] Downloaded content version {id}"
                        " into {path}".format(
                            item=f"{item}",
                            total=queue_size,
                            worker=worker_num,
                            usage=api_usage,
                            id=version.id,
                            path=download_path,
                        )
                    )
                self.wait_if_api_usage_limit(worker_num=worker_num, verbose=verbose)
            except Exception as e:
                click.echo(
                    "[W:{worker}][API usage: {usage:.2f}%] [ERROR] Failed to download content version {id}: {error}"
                    .format(
                        id=queue_item[0].id,
                        error=e,
                        worker=worker_num,
                        usage=api_usage if api_usage is not None else 0.0,
                    )
                )
            finally:
                queue.task_done()

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
