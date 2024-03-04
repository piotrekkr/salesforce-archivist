import concurrent.futures
import csv
import os
import shutil
import threading
from time import sleep
from typing import Generator, Any

import click

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList


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
        data_dir: str,
    ):
        self._document_link_list = document_link_list
        self._content_version_list = content_version_list
        self._data_dir = data_dir
        self._to_download: list[tuple[ContentVersion, str]] | None = None

    def _generate_download_list(self) -> list[tuple[ContentVersion, str]]:
        if self._to_download is None:
            self._to_download = []
            for link in self._document_link_list:
                for version in self._content_version_list.get_content_versions_for_link(link):
                    path = os.path.join(
                        self._data_dir,
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


class DownloadStats:
    def __init__(self) -> None:
        self._total: int = 0
        self._processed: int = 0
        self._errors: int = 0
        self._size: int = 0

    def initialize(self, total: int = 0) -> None:
        self._total = total
        self._processed = 0
        self._errors = 0
        self._size = 0

    def add_processed(self, size: int, error: bool = False) -> None:
        self._processed += 1
        self._total = max(self._total, self._processed)
        self._size += size
        if error:
            self._errors += 1

    @property
    def total(self) -> int:
        return self._total

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def errors(self) -> int:
        return self._errors

    @property
    def size(self) -> int:
        return self._size


class ContentVersionDownloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        downloaded_version_list: DownloadedContentVersionList,
        max_api_usage_percent: float | None = None,
        wait_sec: int = 300,
        max_workers: int | None = None,
    ):
        self._client = sf_client
        self._downloaded_versions_list = downloaded_version_list
        self._max_api_usage_percent = max_api_usage_percent
        self._wait_sec = wait_sec
        self._stats = DownloadStats()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._max_workers = max_workers

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

        percent = self._stats.processed / self._stats.total * 100 if self._stats.total > 0 else 0.0
        item_padded = "{{:{width}d}}".format(width=len(str(self._stats.total))).format(self._stats.processed)

        click.secho(
            "[{emoji} {downloaded}/{total} {percent:6.2f}%] [☁️{usage:6.2f}%] {msg}".format(
                emoji="✓" if not error else "✗",
                downloaded=item_padded,
                percent=percent,
                total=self._stats.total,
                usage=api_usage,
                msg=msg,
            ),
            fg="red" if error else None,
        )

    def download_or_wait(self, version: ContentVersion, download_path: str) -> None:
        msg = "[OK] Downloaded version {id} into {path}".format(
            id=version.id,
            path=download_path,
        )
        error = False
        try:
            self._wait_if_api_usage_limit()
            self.download_content_version_from_sf(version=version, download_path=download_path)
        except StopDownloadException:
            msg = "[ERROR] Stop signal received. Graceful shutdown."
            error = True
        except Exception as e:
            msg = "[ERROR] Failed to download version {id}: {error}".format(id=version.id, error=e)
            error = True
        finally:
            with self._lock:
                self._stats.add_processed(size=version.content_size, error=error)
                self._print_download_msg(msg, error=error)

    def download(self, download_list: DownloadContentVersionList) -> DownloadStats:
        self._stats.initialize(total=len(download_list))
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                for version, download_path in download_list:
                    executor.submit(self.download_or_wait, version=version, download_path=download_path)
        except KeyboardInterrupt as e:
            self._stop_event.set()
            executor.shutdown(wait=True, cancel_futures=True)
            raise e
        return self._stats

    def _wait_if_api_usage_limit(self) -> None:
        if self._max_api_usage_percent is not None:
            usage = self._client.get_api_usage()
            while usage.percent >= self._max_api_usage_percent:
                self._print_download_msg(msg="[NOTICE] Waiting for API limit to drop.")
                for counter in range(self._wait_sec):
                    # check every second if stop signal was received, and if so,
                    # raise exception to stop current download
                    if self._stop_event.is_set():
                        raise StopDownloadException
                    sleep(1)
                usage = self._client.get_api_usage(refresh=True)


class StopDownloadException(Exception):
    pass
