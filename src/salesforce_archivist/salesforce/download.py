from __future__ import annotations

import concurrent.futures
import csv
import os
import shutil
import threading
from time import sleep
from typing import TYPE_CHECKING, Generator

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

    def __iter__(self) -> Generator[tuple[ContentVersion, str], None, None]:
        for link in self._document_link_list:
            for version in self._content_version_list.get_content_versions_for_link(link):
                path = os.path.join(
                    self._archivist_obj.data_dir,
                    "files",
                    link.download_dir_name,
                    version.filename,
                )
                yield version, path


class ContentVersionDownloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        download_content_version_list: DownloadContentVersionList,
        downloaded_version_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
        wait_sec: int = 300,
    ):
        self._client = sf_client
        self._downloaded_versions_list = downloaded_version_list
        self._max_api_usage_percent = max_api_usage_percent
        self._download_list: list[tuple[ContentVersion, str]] = [vp for vp in download_content_version_list]
        self._downloaded_list: list[tuple[ContentVersion, str]] = []
        self._wait_sec = wait_sec

    def _download_from_salesforce(self, version: ContentVersion, download_path: str) -> None:
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
        total_size = len(self._download_list)
        try:
            api_usage: float = self._client.get_api_usage().percent
        except Exception:
            api_usage = 0.0

        downloaded_size = len(self._downloaded_list)
        percent = downloaded_size / total_size * 100
        item_padded = "{{:{width}d}}".format(width=len(str(total_size))).format(downloaded_size)
        click.secho(
            "[{emoji} {downloaded}/{total} {percent:6.2f}%] [☁️{usage:6.2f}%] {msg}".format(
                emoji="💾" if not error else "❌",
                downloaded=item_padded,
                percent=percent,
                total=total_size,
                usage=api_usage,
                msg=msg,
            ),
            fg="red" if error else None,
        )

    def _download_or_wait(self, version: ContentVersion, download_path: str, lock: threading.Lock) -> None:
        msg = "[OK] Downloaded content version {id} into {path}".format(
            id=version.id,
            path=download_path,
        )
        error = False
        try:
            self._download_from_salesforce(version=version, download_path=download_path)
            self._client.get_api_usage()
            self._wait_if_api_usage_limit()
        except Exception as e:
            msg = "[ERROR] Failed to download content version {id}: {error}".format(
                id=version.id,
                error=e,
            )
            error = True
        finally:
            with lock:
                self._downloaded_list.append((version, download_path))
                self._print_download_msg(msg, error=error)

    def download(self, max_workers: int = 5) -> None:
        lock = threading.Lock()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for version, download_path in self._download_list:
                executor.submit(self._download_or_wait, version=version, download_path=download_path, lock=lock)

    def _wait_if_api_usage_limit(self) -> None:
        if self._max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= self._max_api_usage_percent:
                self._print_download_msg(msg="[NOTICE] Waiting for API limit to drop.")
                sleep(self._wait_sec)
                usage = self._client.get_api_usage(refresh=True)
