import concurrent.futures
import csv
import hashlib
import os
import threading

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


class ContentVersionDownloadValidator:
    def __init__(
        self,
        download_content_version_list: DownloadContentVersionList,
        validated_content_version_list: ValidatedContentVersionList,
    ):
        self._versions_list: list[tuple[ContentVersion, str]] = [vp for vp in download_content_version_list]
        self._validated_list = validated_content_version_list
        self._stats = {
            "total": len(self._versions_list),
            "processed": 0,
            "invalid": 0,
        }

    def _print_validated_msg(self, msg: str, error: bool = False) -> None:
        total_size = self._stats["total"]
        processed = self._stats["processed"]
        percent = processed / total_size * 100
        item_padded = "{{:{width}d}}".format(width=len(str(total_size))).format(processed)
        click.secho(
            "[{emoji} {checked}/{total} {percent:6.2f}%] {msg}".format(
                emoji="✅" if not error else "❌", checked=item_padded, percent=percent, total=total_size, msg=msg
            ),
            fg="red" if error else None,
        )

    @staticmethod
    def _calculate_md5(path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            while chunk := f.read(4096):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _validate(self, version: ContentVersion, download_path: str, lock: threading.Lock) -> None:
        msg = "[ OK ] {id} => {path}".format(
            id=version.id,
            path=download_path,
        )
        valid = True
        try:
            if not os.path.exists(download_path):
                msg = "[ KO ] {id} => File does not exist: {path}".format(id=version.id, path=download_path)
                valid = False
            elif (validated_version := self._validated_list.get_version(download_path)) is not None:
                if version.checksum != validated_version.checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    valid = False
            else:
                checksum = self._calculate_md5(download_path)
                if version.checksum != checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    valid = False
                self._validated_list.add_version(ValidatedContentVersion(path=download_path, checksum=checksum))
        except Exception as e:
            msg = "[ KO ] {id} => Exception: {e}".format(id=version.id, e=e)
            valid = False
        finally:
            with lock:
                self._stats["processed"] += 1
                if not valid:
                    self._stats["invalid"] += 1
                self._print_validated_msg(msg, error=not valid)

    def validate(self, max_workers: int = 5) -> dict[str, int]:
        lock = threading.Lock()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for version, download_path in self._versions_list:
                executor.submit(self._validate, version=version, download_path=download_path, lock=lock)
        return self._stats
