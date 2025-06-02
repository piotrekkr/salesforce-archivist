import datetime
import os
from typing import Any, Dict

import click
import humanize
from pydantic import BaseModel, Field, field_validator, ValidationInfo, computed_field
from typing import Optional, Annotated
from simple_salesforce import Salesforce as SalesforceClient

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.download import (
    DownloadContentVersionList,
    DownloadedList,
    DownloadAttachmentList,
    DownloadStats,
)
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ValidatedList, ValidationStats


class ArchivistObject(BaseModel):
    data_dir: Annotated[str, Field(min_length=1)]
    obj_type: Annotated[str, Field(min_length=1)]
    modified_date_lt: Optional[datetime.datetime] = None
    modified_date_gt: Optional[datetime.datetime] = None
    dir_name_field: Optional[str] = None
    extra_soql_condition: Optional[str] = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.data_dir, self.dir_name_field, self.obj_type, self.modified_date_gt, self.modified_date_lt) == (
            other.data_dir,
            other.dir_name_field,
            other.obj_type,
            other.modified_date_gt,
            other.modified_date_lt,
        )

    # https://github.com/python/mypy/issues/14461
    @computed_field  # type: ignore[misc]
    @property
    def obj_dir(self) -> str:
        return os.path.join(self.data_dir, self.obj_type)


class ArchivistAuth(BaseModel):
    instance_url: Annotated[str, Field(min_length=1)]
    username: Annotated[str, Field(min_length=1)]
    consumer_key: Annotated[str, Field(min_length=1)]
    private_key: Annotated[str, Field(min_length=1)]


class ArchivistConfig(BaseModel):
    auth: ArchivistAuth
    data_dir: Annotated[str, Field(min_length=1)]
    max_api_usage_percent: Optional[Annotated[float, Field(gt=0.0, le=100.0)]] = None
    max_workers: Optional[Annotated[int, Field(gt=0)]] = None
    modified_date_gt: Optional[datetime.datetime] = None
    modified_date_lt: Optional[datetime.datetime] = None
    objects: Dict[str, ArchivistObject]

    @field_validator("objects", mode="before")
    @classmethod
    def serialize_categories(cls, objects: dict, info: ValidationInfo) -> dict:
        for obj_type, obj_dict in objects.items():
            obj_dict.update(
                {
                    "obj_type": obj_type,
                    "data_dir": info.data["data_dir"],
                    "modified_date_gt": obj_dict.get("modified_date_gt", info.data["modified_date_gt"]),
                    "modified_date_lt": obj_dict.get("modified_date_lt", info.data["modified_date_lt"]),
                }
            )
        return objects


class Archivist:
    def __init__(
        self,
        data_dir: str,
        objects: dict[str, ArchivistObject],
        sf_client: SalesforceClient,
        max_api_usage_percent: float | None = None,
        max_workers: int | None = None,
    ):
        self._max_api_usage_percent = max_api_usage_percent
        self._objects = objects
        self._data_dir = data_dir
        self._sf_client = sf_client
        self._max_workers = max_workers

    def download(self) -> bool:
        downloaded_content_versions_list = DownloadedList(self._data_dir, "downloaded_versions.csv")
        if downloaded_content_versions_list.data_file_exist():
            downloaded_content_versions_list.load_data_from_file()

        downloaded_attachment_list = DownloadedList(self._data_dir, "downloaded_attachments.csv")
        if downloaded_attachment_list.data_file_exist():
            downloaded_attachment_list.load_data_from_file()

        global_stats = DownloadStats()
        for archivist_obj in self._objects.values():
            obj_type = archivist_obj.obj_type
            if obj_type == "Attachment":
                self._download_attachments(archivist_obj, downloaded_attachment_list, global_stats)
            else:
                self._download_files(archivist_obj, downloaded_content_versions_list, global_stats)

        status = "SUCCESS" if global_stats.errors == 0 else "FAILED"
        color = "green" if global_stats.errors == 0 else "red"
        click.secho(
            "[{status}] Download finished. Processed {processed}/{total} ({processed_size}), {errors} errors.".format(
                status=status,
                processed=global_stats.processed,
                processed_size=humanize.naturalsize(global_stats.size, binary=True),
                errors=global_stats.errors,
                total=global_stats.total,
            ),
            fg=color,
        )
        return global_stats.errors == 0

    def _download_files(
        self,
        archivist_obj: ArchivistObject,
        downloaded_list: DownloadedList,
        global_stats: DownloadStats,
    ) -> None:
        obj_type = archivist_obj.obj_type
        salesforce = Salesforce(
            archivist_obj=archivist_obj,
            client=SalesforceApiClient(self._sf_client),
            max_api_usage_percent=self._max_api_usage_percent,
        )
        self._print_msg(msg="Downloading document link list.", obj_type=obj_type)
        document_link_list = salesforce.load_content_document_link_list()
        self._print_msg(msg="Done.", obj_type=obj_type)
        self._print_msg(msg="Downloading content version list.", obj_type=obj_type)
        content_version_list = salesforce.load_content_version_list(document_link_list=document_link_list)
        self._print_msg(msg="Done.", obj_type=obj_type)
        self._print_msg(msg="Downloading files.", obj_type=obj_type)
        download_list = DownloadContentVersionList(
            document_link_list=document_link_list,
            content_version_list=content_version_list,
            data_dir=archivist_obj.obj_dir,
        )
        stats = salesforce.download_files(
            download_list=download_list,
            downloaded_list=downloaded_list,
            max_workers=self._max_workers,
        )
        global_stats.combine(stats)

    def _download_attachments(
        self, archivist_obj: ArchivistObject, downloaded_list: DownloadedList, global_stats: DownloadStats
    ) -> None:
        obj_type = archivist_obj.obj_type
        salesforce = Salesforce(
            archivist_obj=archivist_obj,
            client=SalesforceApiClient(self._sf_client),
            max_api_usage_percent=self._max_api_usage_percent,
        )
        self._print_msg(msg="Downloading attachment list.", obj_type=obj_type)
        attachment_list = salesforce.load_attachment_list()
        self._print_msg(msg="Done.", obj_type=obj_type)
        self._print_msg(msg="Downloading files.", obj_type=obj_type)
        download_list = DownloadAttachmentList(
            attachment_list=attachment_list,
            data_dir=archivist_obj.obj_dir,
        )
        stats = salesforce.download_files(
            download_list=download_list,
            downloaded_list=downloaded_list,
            max_workers=self._max_workers,
        )
        global_stats.combine(stats)

    def _validate_content_versions_download(
        self, archivist_obj: ArchivistObject, validated_list: ValidatedList, global_stats: ValidationStats
    ) -> bool:
        salesforce = Salesforce(
            archivist_obj=archivist_obj,
            client=SalesforceApiClient(self._sf_client),
            max_api_usage_percent=self._max_api_usage_percent,
        )
        document_link_list = salesforce.load_content_document_link_list()
        content_version_list = salesforce.load_content_version_list(
            document_link_list=document_link_list,
        )
        download_list = DownloadContentVersionList(
            document_link_list=document_link_list,
            content_version_list=content_version_list,
            data_dir=archivist_obj.obj_dir,
        )
        stats = salesforce.validate_download(
            download_list=download_list,
            validated_list=validated_list,
            max_workers=self._max_workers,
        )
        global_stats.combine(stats)
        return stats.invalid == 0

    def _validate_attachments_download(
        self, archivist_obj: ArchivistObject, validated_list: ValidatedList, global_stats: ValidationStats
    ) -> bool:
        salesforce = Salesforce(
            archivist_obj=archivist_obj,
            client=SalesforceApiClient(self._sf_client),
            max_api_usage_percent=self._max_api_usage_percent,
        )
        attachment_list = salesforce.load_attachment_list()
        download_list = DownloadAttachmentList(
            attachment_list=attachment_list,
            data_dir=archivist_obj.obj_dir,
        )
        stats = salesforce.validate_download(
            download_list=download_list,
            validated_list=validated_list,
            max_workers=self._max_workers,
        )
        global_stats.combine(stats)
        return stats.invalid == 0

    def validate(self) -> bool:
        validated_list = ValidatedList(self._data_dir)
        if validated_list.data_file_exist():
            validated_list.load_data_from_file()
        global_stats = ValidationStats()
        for archivist_obj in self._objects.values():
            if archivist_obj.obj_type == "Attachment":
                self._validate_attachments_download(archivist_obj, validated_list, global_stats)
            else:
                self._validate_content_versions_download(archivist_obj, validated_list, global_stats)
        status = "SUCCESS" if global_stats.invalid == 0 else "FAILED"
        color = "green" if global_stats.invalid == 0 else "red"
        click.secho(
            "[{status}] Download validation finished. Processed {processed}/{total}, {invalid} errors.".format(
                status=status, processed=global_stats.processed, invalid=global_stats.invalid, total=global_stats.total
            ),
            fg=color,
        )
        return global_stats.invalid == 0

    @staticmethod
    def _print_msg(msg: str, obj_type: str, fg: str | None = None) -> None:
        click.secho("[{obj_type}] {msg}".format(obj_type=obj_type, msg=msg), fg=fg)
