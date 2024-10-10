import concurrent.futures
import csv
import os
import shutil
import threading
from time import sleep
from typing import Generator, Any, Union, Self

import click

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.attachment import Attachment, AttachmentList
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList


class DownloadedSalesforceObject:
    def __init__(self, obj_id: str, path: str):
        self._id = obj_id
        self._path = path

    @property
    def id(self) -> str:
        return self._id

    @property
    def path(self) -> str:
        return self._path

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.id, self.path) == (other.id, other.path)


class DownloadedList:
    def __init__(self, data_dir: str, file_name: str):
        self._data: dict[str, DownloadedSalesforceObject] = {}
        self._path = os.path.join(data_dir, file_name)

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                obj = DownloadedSalesforceObject(
                    obj_id=row[0],
                    path=row[1],
                )
                self.add(obj=obj)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Id", "Path on disk"])
            for _, sf_obj in self._data.items():
                writer.writerow(
                    [
                        sf_obj.id,
                        sf_obj.path,
                    ]
                )

    def add(self, obj: DownloadedSalesforceObject) -> None:
        self._data[obj.id] = obj

    def is_downloaded(self, obj: Union[ContentVersion, Attachment]) -> bool:
        return obj.id in self._data

    def get(self, obj: Union[ContentVersion, Attachment]) -> DownloadedSalesforceObject | None:
        return self._data.get(obj.id)

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


class DownloadAttachmentList:
    def __init__(
        self,
        attachment_list: AttachmentList,
        data_dir: str,
    ):
        self._attachment_list = attachment_list
        self._data_dir = data_dir
        self._to_download: list[tuple[Attachment, str]] | None = None

    def _generate_download_list(self) -> list[tuple[Attachment, str]]:
        if self._to_download is None:
            self._to_download = []

            for attachment in self._attachment_list:
                path = os.path.join(
                    self._data_dir,
                    "files",
                    attachment.parent_id,
                    attachment.filename,
                )
                self._to_download.append((attachment, path))
        return self._to_download

    def __iter__(self) -> Generator[tuple[Attachment, str], None, None]:
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

    def combine(self, other: Self) -> Self:
        self._total += other.total
        self._processed += other.processed
        self._errors += other.errors
        self._size += other.size
        return self


class Downloader:
    def __init__(
        self,
        sf_client: SalesforceApiClient,
        max_api_usage_percent: float | None = None,
        wait_sec: int = 300,
        max_workers: int | None = None,
    ):
        self._client = sf_client
        self._max_api_usage_percent = max_api_usage_percent
        self._wait_sec = wait_sec
        self._stats = DownloadStats()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._max_workers = max_workers

    def _download_content_version_from_sf(
        self, downloaded_versions_list: DownloadedList, version: ContentVersion, download_path: str
    ) -> None:
        downloaded_version = downloaded_versions_list.get(version)
        # file exist under the path that we want to download into
        if os.path.exists(download_path):
            # if no version exist in downloaded list add a new downloaded version with this path
            if downloaded_version is None:
                downloaded_version = DownloadedSalesforceObject(
                    obj_id=version.id,
                    path=download_path,
                )
                downloaded_versions_list.add(downloaded_version)

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

            downloaded_version = DownloadedSalesforceObject(
                obj_id=version.id,
                path=download_path,
            )
            downloaded_versions_list.add(downloaded_version)

    def _download_attachment_from_sf(
        self, downloaded_attachment_list: DownloadedList, attachment: Attachment, download_path: str
    ) -> None:
        result = self._client.download_attachment(attachment)
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        with open(download_path, "wb") as file:
            for chunk in result.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        downloaded_attachment = DownloadedSalesforceObject(
            obj_id=attachment.id,
            path=download_path,
        )
        downloaded_attachment_list.add(downloaded_attachment)

    def download_from_sf(
        self,
        downloaded_list: DownloadedList,
        download_obj: Union[ContentVersion, Attachment],
        download_path: str,
    ) -> None:
        if isinstance(download_obj, ContentVersion):
            self._download_content_version_from_sf(
                downloaded_versions_list=downloaded_list,
                version=download_obj,
                download_path=download_path,
            )
        elif isinstance(download_obj, Attachment):
            self._download_attachment_from_sf(
                downloaded_attachment_list=downloaded_list,
                attachment=download_obj,
                download_path=download_path,
            )
        else:
            raise ValueError("Unknown object type provided {type}".format(type=type(download_obj)))

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

    def download_or_wait(
        self,
        downloaded_list: DownloadedList,
        download_obj: Union[ContentVersion, Attachment],
        download_path: str,
    ) -> None:
        msg = "[OK] Downloaded object {id} into {path}".format(
            id=download_obj.id,
            path=download_path,
        )
        error = False
        try:
            self._wait_if_api_usage_limit()
            self.download_from_sf(
                downloaded_list=downloaded_list, download_obj=download_obj, download_path=download_path
            )
        except StopDownloadException:
            msg = "[ERROR] Stop signal received. Graceful shutdown."
            error = True
        except Exception as e:
            msg = "[ERROR] Failed to download object {id}: {error}".format(id=download_obj.id, error=e)
            error = True
        finally:
            with self._lock:
                self._stats.add_processed(size=download_obj.content_size, error=error)
                self._print_download_msg(msg, error=error)

    def download(
        self,
        downloaded_list: DownloadedList,
        download_list: Union[DownloadContentVersionList, DownloadAttachmentList],
    ) -> DownloadStats:
        self._stats.initialize(total=len(download_list))
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                for download_obj, download_path in download_list:
                    executor.submit(
                        self.download_or_wait,
                        downloaded_list=downloaded_list,
                        download_obj=download_obj,
                        download_path=download_path,
                    )
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
