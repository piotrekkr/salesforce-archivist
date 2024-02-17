import csv
import os.path
import re
from typing import Any, Generator

from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink


class ContentVersion:
    def __init__(self, id: str, document_id: str, title: str, extension: str, checksum: str, version_number: int):
        self._id = id
        self._document_id = document_id
        self._title = title
        self._extension = extension
        self._checksum = checksum
        self._version_number = version_number

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
    def version_number(self) -> int:
        return self._version_number

    @property
    def filename(self) -> str:
        return "{doc_id}_{version_number}_{id}_{title}.{extension}".format(
            doc_id=self.document_id,
            id=self.id,
            # TODO make it configurable
            title=re.sub(r'[/\\?%*:|"<>]', "-", self.title),
            extension=self.extension,
            version_number=self.version_number,
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
            self.version_number,
        ) == (other.id, other.document_id, other.title, other.extension, other.checksum, other.version_number)


class ContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ContentVersion] = {}
        self._path = os.path.join(data_dir, "content_versions.csv")
        self._doc_versions_map: dict[str, set[str]] = {}

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
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
                    version_number=int(row[5]),
                )
                self.add_version(version)

    def save(self) -> None:
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension", "VersionNumber"])
            for version_id, version in self._data.items():
                writer.writerow(
                    [
                        version.id,
                        version.document_id,
                        version.checksum,
                        version.title,
                        version.extension,
                        version.version_number,
                    ]
                )

    def get_content_version(self, version_id: str) -> ContentVersion | None:
        return self._data.get(version_id)

    def add_version(self, version: ContentVersion) -> None:
        if version.document_id not in self._doc_versions_map:
            self._doc_versions_map[version.document_id] = set()
        self._doc_versions_map[version.document_id].add(version.id)
        self._data[version.id] = version

    def get_content_versions_for_link(self, link: ContentDocumentLink) -> Generator[ContentVersion, None, None]:
        if link.content_document_id in self._doc_versions_map:
            for version_id in self._doc_versions_map[link.content_document_id]:
                yield self._data[version_id]

    def __len__(self) -> int:
        return len(self._data)

    @property
    def path(self) -> str:
        return self._path
