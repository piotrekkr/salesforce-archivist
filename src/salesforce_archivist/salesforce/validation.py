import concurrent.futures
import csv
import hashlib
import os
import threading
from typing import Any, Self, Union, Optional

import click

from salesforce_archivist.salesforce.attachment import Attachment
from salesforce_archivist.salesforce.content_version import ContentVersion
from salesforce_archivist.salesforce.download import DownloadContentVersionList, DownloadAttachmentList


class ValidatedFile:
    def __init__(self, path: str, checksum: Optional[str] = None, content_size: Optional[int] = None):
        self._path = path
        self._content_size = content_size
        self._checksum = checksum
        if self._checksum is None and self._content_size is None:
            raise ValueError("Either checksum or content_size must be provided")

    @property
    def path(self) -> str:
        return self._path

    @property
    def content_size(self) -> Optional[int]:
        return self._content_size

    @property
    def checksum(self) -> Optional[str]:
        return self._checksum

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.content_size, self._checksum, self.path) == (other.content_size, other._checksum, other.path)


class ValidatedList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ValidatedFile] = {}
        self._path = os.path.join(data_dir, "validated_files.csv")

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                checksum = row[0] if row[0] != "" else None
                size = int(row[1]) if row[1] != "" else None
                validated_file = ValidatedFile(path=row[2], checksum=checksum, content_size=size)
                self.add(validated_file)

    def save(self) -> None:
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Checksum", "Content Size", "Path"])
            for _, validated_file in self._data.items():
                writer.writerow(
                    [
                        validated_file.checksum if validated_file.checksum is not None else "",
                        validated_file.content_size if validated_file.content_size is not None else "",
                        validated_file.path,
                    ]
                )

    def add(self, validated_file: ValidatedFile) -> None:
        self._data[validated_file.path] = validated_file

    def is_validated(self, path: str) -> bool:
        return path in self._data

    def get(self, path: str) -> ValidatedFile | None:
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

    def combine(self, other: Self) -> None:
        self._total += other.total
        self._processed += other.processed
        self._invalid += other.invalid


class DownloadValidator:
    def __init__(self, validated_list: ValidatedList, max_workers: int | None = None):
        self._validated_list = validated_list
        self._stats = ValidationStats()
        self._lock = threading.Lock()
        self._max_workers = max_workers

    def _print_validated_msg(self, msg: str, invalid: bool = False) -> None:
        percent = self._stats.processed / self._stats.total * 100 if self._stats.total > 0 else 0.0
        item_padded = "{{:{width}d}}".format(width=len(str(self._stats.total))).format(self._stats.processed)
        click.secho(
            "[{emoji} {checked}/{total} {percent:6.2f}%] {msg}".format(
                emoji="✓" if not invalid else "✗",
                checked=item_padded,
                percent=percent,
                total=self._stats.total,
                msg=msg,
            ),
            fg="red" if invalid else None,
        )

    @staticmethod
    def _calculate_size(path: str) -> int:
        return os.path.getsize(path)

    @staticmethod
    def _calculate_md5(path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            while chunk := f.read(4096):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _validate_version(self, version: ContentVersion, download_path: str) -> bool:
        valid = True
        msg = "[ OK ] {id} => {path}".format(id=version.id, path=download_path)
        try:
            if not os.path.exists(download_path):
                msg = "[ KO ] {id} => File does not exist: {path}".format(id=version.id, path=download_path)
                valid = False
            elif (validated := self._validated_list.get(download_path)) is not None:
                if version.checksum != validated.checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    valid = False
            else:
                checksum = self._calculate_md5(download_path)
                if version.checksum != checksum:
                    msg = "[ KO ] {id} => checksum invalid: {path}".format(id=version.id, path=download_path)
                    valid = False
                self._validated_list.add(
                    ValidatedFile(path=download_path, checksum=checksum, content_size=version.content_size)
                )
        except Exception as e:
            msg = "[ KO ] {id} => Exception: {e}".format(id=version.id, e=e)
            valid = False
        finally:
            with self._lock:
                self._stats.add_processed(invalid=not valid)
                self._print_validated_msg(msg, invalid=not valid)

        return valid

    def _validate_attachment(self, attachment: Attachment, download_path: str) -> bool:
        valid = True
        msg = "[ OK ] {id} => {path}".format(id=attachment.id, path=download_path)
        try:
            if not os.path.exists(download_path):
                msg = "[ KO ] {id} => File does not exist: {path}".format(id=attachment.id, path=download_path)
                valid = False
            elif (validated := self._validated_list.get(download_path)) is not None:
                if attachment.content_size != validated.content_size:
                    msg = "[ KO ] {id} => size invalid: {path}".format(id=attachment.id, path=download_path)
                    valid = False
            else:
                size = self._calculate_size(download_path)
                if attachment.content_size != size:
                    msg = "[ KO ] {id} => size invalid: {path}".format(id=attachment.id, path=download_path)
                    valid = False
                self._validated_list.add(
                    ValidatedFile(path=download_path, checksum=None, content_size=attachment.content_size)
                )
        except Exception as e:
            msg = "[ KO ] {id} => Exception: {e}".format(id=attachment.id, e=e)
            valid = False
        finally:
            with self._lock:
                self._stats.add_processed(invalid=not valid)
                self._print_validated_msg(msg, invalid=not valid)
        return valid

    def validate_object(self, obj: Union[ContentVersion, Attachment], download_path: str) -> bool:
        if isinstance(obj, ContentVersion):
            return self._validate_version(obj, download_path)
        elif isinstance(obj, Attachment):
            return self._validate_attachment(obj, download_path)
        else:
            msg = "[ KO ] {id} => Invalid object type provided: {type}".format(id=obj.id, type=type(obj))
            with self._lock:
                self._stats.add_processed(invalid=True)
                self._print_validated_msg(msg, invalid=True)
            return False

    def validate(self, download_list: Union[DownloadContentVersionList, DownloadAttachmentList]) -> ValidationStats:
        self._stats.initialize(total=len(download_list))
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                for obj, download_path in download_list:
                    executor.submit(self.validate_object, obj=obj, download_path=download_path)
        except KeyboardInterrupt as e:
            executor.shutdown(wait=True, cancel_futures=True)
            raise e

        return self._stats
