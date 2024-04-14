from __future__ import annotations

import csv
import glob
import os.path
from math import ceil
from typing import TYPE_CHECKING

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink, ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import (
    ContentVersionDownloader,
    DownloadContentVersionList,
    DownloadedContentVersionList,
    DownloadStats,
)
from salesforce_archivist.salesforce.validation import (
    ContentVersionDownloadValidator,
    ValidatedContentVersionList,
    ValidationStats,
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
        select_list = ["LinkedEntityId", "ContentDocumentId"]
        if self._archivist_obj.dir_name_field is not None and self._archivist_obj.dir_name_field not in select_list:
            select_list.append(self._archivist_obj.dir_name_field)
        where_list = ["LinkedEntity.Type = '{obj_type}'".format(obj_type=self._archivist_obj.obj_type)]
        if self._archivist_obj.modified_date_lt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate < {date}".format(
                    date=self._archivist_obj.modified_date_lt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        if self._archivist_obj.modified_date_gt is not None:
            where_list.append(
                "ContentDocument.ContentModifiedDate > {date}".format(
                    date=self._archivist_obj.modified_date_gt.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
        return "SELECT {fields} FROM ContentDocumentLink WHERE {where}".format(
            fields=", ".join(select_list), where=" AND ".join(where_list)
        )

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
                    link = ContentDocumentLink(
                        linked_entity_id=row[0],
                        content_document_id=row[1],
                        download_dir_name=row[2] if self._archivist_obj.dir_name_field is not None else None,
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
                        id=row[0],
                        document_id=row[1],
                        checksum=row[2],
                        title=row[3],
                        extension=row[4],
                        version_number=int(row[5]),
                        content_size=int(row[6]),
                    )
                    content_version_list.add_version(version)

    def download_files(
        self,
        download_content_version_list: DownloadContentVersionList,
        downloaded_content_version_list: DownloadedContentVersionList,
        max_workers: int | None = None,
    ) -> DownloadStats:
        try:
            downloader = ContentVersionDownloader(
                sf_client=self._client,
                downloaded_version_list=downloaded_content_version_list,
                max_api_usage_percent=self._max_api_usage_percent,
                max_workers=max_workers,
            )
            return downloader.download(download_list=download_content_version_list)
        finally:
            downloaded_content_version_list.save()

    @staticmethod
    def validate_download(
        download_content_version_list: DownloadContentVersionList,
        validated_content_version_list: ValidatedContentVersionList,
        max_workers: int | None = None,
    ) -> ValidationStats:
        try:
            validator = ContentVersionDownloadValidator(
                validated_content_version_list=validated_content_version_list,
                max_workers=max_workers,
            )
            return validator.validate(download_list=download_content_version_list)
        finally:
            validated_content_version_list.save()
