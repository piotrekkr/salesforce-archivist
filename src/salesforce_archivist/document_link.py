import csv
import os.path


class ContentDocumentLink:
    def __init__(
        self,
        linked_entity_id: str,
        content_document_id: str,
        download_dir_name: str | None = None,
    ):
        self._linked_entity_id = linked_entity_id
        self._content_document_id = content_document_id
        self._download_dir_name = download_dir_name

    @property
    def linked_entity_id(self):
        return self._linked_entity_id

    @property
    def content_document_id(self):
        return self._content_document_id

    @property
    def download_dir_name(self):
        return self._download_dir_name

    def to_csv(self) -> list[str]:
        row = [self.linked_entity_id, self.content_document_id]
        if self._download_dir_name is not None:
            row.append(self._download_dir_name)
        return row

    def __hash__(self):
        return hash(
            (self.linked_entity_id, self.content_document_id, self.download_dir_name)
        )

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.linked_entity_id, self.content_document_id) == (
            other.linked_entity_id,
            other.content_document_id,
        )


class ContentDocumentLinkList:
    def __init__(self, data_dir: str, dir_name_field):
        self._data: dict[str, ContentDocumentLink] = {}
        self._path = os.path.join(data_dir, "document_links.csv")
        self._dir_name_field = dir_name_field
        if os.path.exists(self._path):
            self._load_data()

    def _load_data(self):
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                link = ContentDocumentLink(
                    linked_entity_id=row[0],
                    content_document_id=row[1],
                    download_dir_name=row[2] if len(row) == 3 else None,
                )
                self.add_link(link)

    def save(self):
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            header = ["LinkedEntityId", "ContentDocumentId"]
            if self._dir_name_field is not None:
                header.append(self._dir_name_field)
            writer.writerow(header)
            for doc_link_id, link in self._data.items():
                row = [
                    link.linked_entity_id,
                    link.content_document_id,
                ]
                if link.download_dir_name is not None:
                    row.append(link.download_dir_name)
                writer.writerow(row)

    def add_link(self, doc_link: ContentDocumentLink):
        key = "{linked_id}_{document_id}".format(
            linked_id=doc_link.linked_entity_id,
            document_id=doc_link.content_document_id,
        )
        self._data[key] = doc_link

    @property
    def path(self):
        return self._path

    def get_links(self) -> dict[str, ContentDocumentLink]:
        return self._data

    def __len__(self):
        return len(self._data)
