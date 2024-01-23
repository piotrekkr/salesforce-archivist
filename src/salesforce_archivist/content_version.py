import csv
import os.path
import re
from typing import Any

from salesforce_archivist.document_link import ContentDocumentLink


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
