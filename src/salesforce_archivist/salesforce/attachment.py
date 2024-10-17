import csv
import re
import os.path
from typing import Any, Generator


class Attachment:
    def __init__(
        self,
        attachment_id: str,
        parent_id: str,
        name: str,
        content_size: int,
    ):
        self._id = attachment_id
        self._parent_id = parent_id
        self._name = name
        self._content_size = content_size

    @property
    def id(self) -> str:
        return self._id

    @property
    def parent_id(self) -> str:
        return self._parent_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def content_size(self) -> int:
        return self._content_size

    @property
    def filename(self) -> str:
        return "{id}_{name}".format(
            id=self.id,
            # TODO make it configurable
            name=re.sub(r'[/\\?%*:|"<>]', "-", self.name),
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            self.id,
            self.parent_id,
            self.name,
            self.content_size,
        ) == (
            other.id,
            other.parent_id,
            other.name,
            other.content_size,
        )


class AttachmentList:
    def __init__(self, data_dir: str):
        self._data: dict[str, Attachment] = {}
        self._path = os.path.join(data_dir, "attachments.csv")
        self._parent_attachment_map: dict[str, set[str]] = {}

    def data_file_exist(self) -> bool:
        return os.path.exists(self._path)

    def load_data_from_file(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                attachment = Attachment(
                    attachment_id=row[0],
                    parent_id=row[1],
                    content_size=int(row[2]),
                    name=row[3],
                )
                self.add_attachment(attachment)

    def save(self) -> None:
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Id", "ParentId", "ContentSize", "Name"])
            for attachment_id, attachment in self._data.items():
                writer.writerow(
                    [
                        attachment.id,
                        attachment.parent_id,
                        attachment.content_size,
                        attachment.name,
                    ]
                )

    def get_attachment(self, attachment_id: str) -> Attachment | None:
        return self._data.get(attachment_id)

    def add_attachment(self, attachment: Attachment) -> None:
        if attachment.parent_id not in self._parent_attachment_map:
            self._parent_attachment_map[attachment.parent_id] = set()
        self._parent_attachment_map[attachment.parent_id].add(attachment.id)
        self._data[attachment.id] = attachment

    def get_attachments_for_parent(self, parent_id: str) -> Generator[Attachment, None, None]:
        if parent_id in self._parent_attachment_map:
            for attachment_id in self._parent_attachment_map[parent_id]:
                yield self._data[attachment_id]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Generator[Attachment, None, None]:
        yield from self._data.values()

    @property
    def path(self) -> str:
        return self._path
