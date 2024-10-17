from __future__ import annotations

import csv
import glob
import os.path
from math import ceil
from typing import TYPE_CHECKING, Union

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.attachment import AttachmentList, Attachment
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink, ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import (
    Downloader,
    DownloadContentVersionList,
    DownloadedList,
    DownloadStats,
    DownloadAttachmentList,
)
from salesforce_archivist.salesforce.validation import (
    ValidationStats,
    DownloadValidator,
    ValidatedList,
)

if TYPE_CHECKING:
    from salesforce_archivist.archivist import ArchivistObject


class Salesforce:
    def __init__(
        self,
        archivist_obj: ArchivistObject,
        client: SalesforceApiClient,
        dir_name_field: str | None = None,
        max_api_usage_percent: float | None = None,
    ):
        self._archivist_obj = archivist_obj
        self._client = client
        self._max_api_usage_percent = max_api_usage_percent
        self._dir_name_field = dir_name_field

    def _init_tmp_dir(self) -> str:
        tmp_dir = os.path.join(self._archivist_obj.obj_dir, "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        for entry in os.scandir(tmp_dir):
            if entry.is_file():
                os.remove(entry.path)
        return tmp_dir

    def _get_content_document_list_query(self) -> str:
        select_list = ["LinkedEntityId", "ContentDocumentId", "LinkedEntity.Type"]
        if self._archivist_obj.dir_name_field is not None and self._archivist_obj.dir_name_field not in select_list:
            select_list.append(self._archivist_obj.dir_name_field)
        where_conditions = []
        if self._archivist_obj.modified_date_lt is not None:
            where_conditions.append(
                "ContentDocument.ContentModifiedDate < {date}".format(
                    date=self._archivist_obj.modified_date_lt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        if self._archivist_obj.modified_date_gt is not None:
            where_conditions.append(
                "ContentDocument.ContentModifiedDate > {date}".format(
                    date=self._archivist_obj.modified_date_gt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        where = ""
        if len(where_conditions):
            where = "WHERE {}".format(" AND ".join(where_conditions))

        # Using WHERE IN and not using filter on `LinkedEntity.Type` is done because of SF restrictions like:
        #
        #   Implementation restriction: ContentDocumentLink requires a filter by a single ID on ContentDocumentId
        #   or LinkedEntityId using the equals operator or multiple ID's using the IN operator.
        #
        #   Implementation restriction: filtering on non-id fields is only permitted when filtering
        #   by ContentDocumentLink.LinkedEntityId using the equal operator.

        return (
            "SELECT {fields} FROM ContentDocumentLink "
            "WHERE ContentDocumentId IN (SELECT Id FROM ContentDocument {where})"
        ).format(fields=", ".join(select_list), where=where)

    def download_content_document_link_list(
        self,
        document_link_list: ContentDocumentLinkList,
        max_records: int = 50000,
    ) -> None:
        tmp_dir = self._init_tmp_dir()
        query = self._get_content_document_list_query()
        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)

        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    # If type is not the same as the object type, skip.
                    # This is a workaround for restriction on ContentDocumentLink filtering directly in query.
                    if row[2] != self._archivist_obj.obj_type:
                        continue
                    link = ContentDocumentLink(
                        linked_entity_id=row[0],
                        content_document_id=row[1],
                        download_dir_name=row[3] if self._archivist_obj.dir_name_field is not None else None,
                    )
                    document_link_list.add_link(link)

    def load_content_document_link_list(self) -> ContentDocumentLinkList:
        document_link_list = ContentDocumentLinkList(
            data_dir=self._archivist_obj.obj_dir, dir_name_field=self._archivist_obj.dir_name_field
        )
        if not document_link_list.data_file_exist():
            self.download_content_document_link_list(document_link_list=document_link_list)
            document_link_list.save()
        else:
            document_link_list.load_data_from_file()

        return document_link_list

    def load_content_version_list(
        self,
        document_link_list: ContentDocumentLinkList,
        batch_size: int = 3000,
    ) -> ContentVersionList:
        content_version_list = ContentVersionList(data_dir=self._archivist_obj.obj_dir)
        if not content_version_list.data_file_exist():
            doc_id_list = [link.content_document_id for link in document_link_list]
            list_size = len(doc_id_list)
            all_batches = ceil(list_size / batch_size)

            for batch in range(1, all_batches + 1):
                start = (batch - 1) * batch_size
                end = start + batch_size
                doc_id_batch = doc_id_list[start:end]
                self.download_content_version_list(
                    document_ids=doc_id_batch,
                    content_version_list=content_version_list,
                )
            content_version_list.save()
        else:
            content_version_list.load_data_from_file()

        return content_version_list

    def download_content_version_list(
        self,
        document_ids: list[str],
        content_version_list: ContentVersionList,
        max_records: int = 50000,
    ) -> None:
        tmp_dir = self._init_tmp_dir()
        query = (
            "SELECT Id, ContentDocumentId, Checksum, Title, FileExtension, VersionNumber, ContentSize "
            "FROM ContentVersion WHERE ContentDocumentId IN ({id_list}) AND ContentSize > 1"
        ).format(id_list=",".join(["'{id}'".format(id=doc_id) for doc_id in document_ids]))
        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)
        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    version = ContentVersion(
                        version_id=row[0],
                        document_id=row[1],
                        checksum=row[2],
                        title=row[3],
                        extension=row[4],
                        version_number=int(row[5]),
                        content_size=int(row[6]),
                    )
                    content_version_list.add_version(version)

    def _get_attachment_list_query(self) -> str:
        select_list = ["Id", "ParentId", "BodyLength", "Name"]
        where_conditions = []
        if self._archivist_obj.modified_date_lt is not None:
            where_conditions.append(
                "LastModifiedDate < {date}".format(
                    date=self._archivist_obj.modified_date_lt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        if self._archivist_obj.modified_date_gt is not None:
            where_conditions.append(
                "LastModifiedDate > {date}".format(
                    date=self._archivist_obj.modified_date_gt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        where = ""
        if len(where_conditions):
            where = "WHERE {}".format(" AND ".join(where_conditions))

        return "SELECT {fields} FROM Attachment {where}".format(fields=", ".join(select_list), where=where).rstrip(" ")

    def download_attachment_list(
        self,
        attachment_list: AttachmentList,
        max_records: int = 50000,
    ) -> None:
        tmp_dir = self._init_tmp_dir()
        query = self._get_attachment_list_query()

        self._client.bulk2(query=query, path=tmp_dir, max_records=max_records)

        for path in glob.glob(os.path.join(tmp_dir, "*.csv")):
            with open(path) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    attachment = Attachment(
                        attachment_id=row[0],
                        parent_id=row[1],
                        content_size=int(row[2]),
                        name=row[3],
                    )
                    attachment_list.add_attachment(attachment)

    def load_attachment_list(self) -> AttachmentList:
        attachment_list = AttachmentList(data_dir=self._archivist_obj.obj_dir)
        if not attachment_list.data_file_exist():
            self.download_attachment_list(attachment_list=attachment_list)
            attachment_list.save()
        else:
            attachment_list.load_data_from_file()

        return attachment_list

    def download_files(
        self,
        downloaded_list: DownloadedList,
        download_list: Union[DownloadContentVersionList, DownloadAttachmentList],
        max_workers: int | None = None,
    ) -> DownloadStats:
        try:
            downloader = Downloader(
                sf_client=self._client,
                max_api_usage_percent=self._max_api_usage_percent,
                max_workers=max_workers,
            )
            return downloader.download(downloaded_list=downloaded_list, download_list=download_list)
        finally:
            downloaded_list.save()

    @staticmethod
    def validate_download(
        download_list: Union[DownloadContentVersionList, DownloadAttachmentList],
        validated_list: ValidatedList,
        max_workers: int | None = None,
    ) -> ValidationStats:
        try:
            validator = DownloadValidator(
                validated_list=validated_list,
                max_workers=max_workers,
            )
            return validator.validate(download_list=download_list)
        finally:
            validated_list.save()
