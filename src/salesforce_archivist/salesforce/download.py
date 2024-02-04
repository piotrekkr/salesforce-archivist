from __future__ import annotations

import concurrent.futures
import csv
import os
import shutil
import threading
from time import sleep
from typing import TYPE_CHECKING, Generator, Any

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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.id, self.document_id, self.path) == (other.id, other.document_id, other.path)


class DownloadedContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, DownloadedContentVersion] = {}
        self._path = os.path.join(data_dir, "downloaded_versions.csv")

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
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
                writer.writerow(
                    [
                        version.id,
                        version.document_id,
                        version.path,
                    ]
                )

    def add_version(self, version: DownloadedContentVersion) -> None:
        self._data[version.id] = version

    def is_downloaded(self, content_version: ContentVersion) -> bool:
        return content_version.id in self._data

    def get_version(self, content_version: ContentVersion) -> DownloadedContentVersion | None:
        return self._data.get(content_version.id)

    @property
    def path(self) -> str:
        return self._path

    def __len__(self) -> int:
        return len(self._data)


class DownloadContentVersionList:
    def __init__(
        self,
        document_link_list: ContentDocumentLinkList,
        content_version_list: ContentVersionList,
        archivist_obj: ArchivistObject,
    ):
        self._document_link_list = document_link_list
        self._content_version_list = content_version_list
        self._archivist_obj = archivist_obj
        self._to_download: list[tuple[ContentVersion, str]] | None = None

    def _generate_download_list(self) -> list[tuple[ContentVersion, str]]:
        if self._to_download is None:
            self._to_download = []
            for link in self._document_link_list:
                for version in self._content_version_list.get_content_versions_for_link(link):
                    path = os.path.join(
                        self._archivist_obj.data_dir,
                        "files",
                        link.download_dir_name,
                        version.filename,
                    )
                    self._to_download.append((version, path))
        return self._to_download

    def __iter__(self) -> Generator[tuple[ContentVersion, str], None, None]:
        yield from self._generate_download_list()

    def __len__(self) -> int:
        return len(self._generate_download_list())


class ContentVersionDownloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        downloaded_version_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
        wait_sec: int = 300,
    ):
        self._client = sf_client
        self._downloaded_versions_list = downloaded_version_list
        self._max_api_usage_percent = max_api_usage_percent
        self._downloaded_list: list[tuple[ContentVersion, str]] = []
        self._wait_sec = wait_sec
        self._total_download_count = 1

    def download_content_version_from_sf(self, version: ContentVersion, download_path: str) -> None:
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

    def _print_download_msg(self, msg: str, error: bool = False) -> None:
        try:
            api_usage: float = self._client.get_api_usage().percent
        except Exception:
            api_usage = 0.0

        downloaded_count = len(self._downloaded_list)
        total_count = self._total_download_count
        percent = downloaded_count / total_count * 100
        item_padded = "{{:{width}d}}".format(width=len(str(total_count))).format(downloaded_count)
        click.secho(
            "[{emoji} {downloaded}/{total} {percent:6.2f}%] [☁️{usage:6.2f}%] {msg}".format(
                emoji="💾" if not error else "❌",
                downloaded=item_padded,
                percent=percent,
                total=total_count,
                usage=api_usage,
                msg=msg,
            ),
            fg="red" if error else None,
        )

    def download_or_wait(self, version: ContentVersion, download_path: str, lock: threading.Lock) -> None:
        msg = "[OK] Downloaded content version {id} into {path}".format(
            id=version.id,
            path=download_path,
        )
        error = False
        try:
            self.download_content_version_from_sf(version=version, download_path=download_path)
            self._client.get_api_usage()
            self._wait_if_api_usage_limit()
        except Exception as e:
            msg = "[ERROR] Failed to download content version {id}: {error}".format(id=version.id, error=e)
            error = True
        finally:
            with lock:
                self._downloaded_list.append((version, download_path))
                self._print_download_msg(msg, error=error)

    def download(self, download_list: DownloadContentVersionList, max_workers: int = 5) -> None:
        try:
            lock = threading.Lock()
            self._total_download_count = len(download_list)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for version, download_path in download_list:
                    executor.submit(self.download_or_wait, version=version, download_path=download_path, lock=lock)
        finally:
            self._total_download_count = 1

    def _wait_if_api_usage_limit(self) -> None:
        if self._max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= self._max_api_usage_percent:
                self._print_download_msg(msg="[NOTICE] Waiting for API limit to drop.")
                sleep(self._wait_sec)
                usage = self._client.get_api_usage(refresh=True)
