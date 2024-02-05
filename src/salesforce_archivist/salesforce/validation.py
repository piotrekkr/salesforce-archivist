import concurrent.futures
import csv
import hashlib
import os
import threading
from typing import Any

import click

from salesforce_archivist.salesforce.content_version import ContentVersion
from salesforce_archivist.salesforce.download import DownloadContentVersionList


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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.checksum, self.path) == (other.checksum, other.path)


class ValidatedContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ValidatedContentVersion] = {}
        self._path = os.path.join(data_dir, "validated_versions.csv")

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                version = ValidatedContentVersion(checksum=row[0], path=row[1])
                self.add_version(version)

    def save(self) -> None:
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Checksum", "Path"])
            for version_id, version in self._data.items():
                writer.writerow(
                    [
                        version.checksum,
                        version.path,
                    ]
                )

    def add_version(self, version: ValidatedContentVersion) -> None:
        self._data[version.path] = version

    def is_validated(self, path: str) -> bool:
        return path in self._data

    def get_version(self, path: str) -> ValidatedContentVersion | None:
        return self._data.get(path)

    @property
    def path(self) -> str:
        return self._path

    def __len__(self) -> int:
        return len(self._data)


class ValidationStats:
    def __init__(self) -> None:
        self._total: int = 0
        self._processed: int = 0
        self._invalid: int = 0

    def initialize(self, total: int = 0) -> None:
        self._total = total
        self._processed = 0
        self._invalid = 0

    def add_processed(self, invalid: bool = False) -> None:
        self._processed += 1
        self._total = max(self._total, self._processed)
        if invalid:
            self._invalid += 1

    @property
    def total(self) -> int:
        return self._total

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def invalid(self) -> int:
        return self._invalid


class ContentVersionDownloadValidator:
    def __init__(
        self,
        validated_content_version_list: ValidatedContentVersionList,
    ):
        self._validated_list = validated_content_version_list
        self._stats = ValidationStats()
        self._lock = threading.Lock()

    def _print_validated_msg(self, msg: str, invalid: bool = False) -> None:
        percent = self._stats.processed / self._stats.total * 100 if self._stats.total > 0 else 0.0
        item_padded = "{{:{width}d}}".format(width=len(str(self._stats.total))).format(self._stats.processed)
        click.secho(
            "[{emoji} {checked}/{total} {percent:6.2f}%] {msg}".format(
                emoji="✅" if not invalid else "❌",
                checked=item_padded,
                percent=percent,
                total=self._stats.total,
                msg=msg,
            ),
            fg="red" if invalid else None,
        )

    @staticmethod
    def _calculate_md5(path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            while chunk := f.read(4096):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def validate_version(self, version: ContentVersion, download_path: str) -> None:
        msg = "[ OK ] {id} => {path}".format(
            id=version.id,
            path=download_path,
        )
        invalid = False
        try:
            if not os.path.exists(download_path):
                msg = "[ KO ] {id} => File does not exist: {path}".format(id=version.id, path=download_path)
                invalid = True
            elif (validated_version := self._validated_list.get_version(download_path)) is not None:
                if version.checksum != validated_version.checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    invalid = True
            else:
                checksum = self._calculate_md5(download_path)
                if version.checksum != checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    invalid = True
                self._validated_list.add_version(ValidatedContentVersion(path=download_path, checksum=checksum))
        except Exception as e:
            msg = "[ KO ] {id} => Exception: {e}".format(id=version.id, e=e)
            invalid = True
        finally:
            with self._lock:
                self._stats.add_processed(invalid=invalid)
                self._print_validated_msg(msg, invalid=invalid)

    def validate(self, download_list: DownloadContentVersionList, max_workers: int = 5) -> ValidationStats:
        self._stats.initialize(total=len(download_list))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for version, download_path in download_list:
                executor.submit(self.validate_version, version=version, download_path=download_path)
        return self._stats
