import datetime
import os
from typing import Any, Dict

import click
import humanize
from pydantic import BaseModel, Field, field_validator, ValidationInfo, computed_field
from typing import Optional
from typing_extensions import Annotated
from simple_salesforce import Salesforce as SalesforceClient

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.download import DownloadContentVersionList, DownloadedContentVersionList
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ValidatedContentVersionList


class ArchivistObject(BaseModel):
    data_dir: Annotated[str, Field(min_length=1)]
    obj_type: Annotated[str, Field(min_length=1)]
    modified_date_lt: Optional[datetime.datetime] = None
    modified_date_gt: Optional[datetime.datetime] = None
    dir_name_field: Optional[str] = None

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
        downloaded_content_versions_list = DownloadedContentVersionList(self._data_dir)
        if downloaded_content_versions_list.data_file_exist():
            downloaded_content_versions_list.load_data_from_file()

        global_stats = {
            "total": 0,
            "processed": 0,
            "errors": 0,
            "size": 0,
        }
        for archivist_obj in self._objects.values():
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
                download_content_version_list=download_list,
                downloaded_content_version_list=downloaded_content_versions_list,
                max_workers=self._max_workers,
            )
            global_stats["total"] += stats.total
            global_stats["processed"] += stats.processed
            global_stats["errors"] += stats.errors
            global_stats["size"] += stats.size

        status = "SUCCESS" if global_stats["errors"] == 0 else "FAILED"
        color = "green" if global_stats["errors"] == 0 else "red"
        click.secho(
            "[{status}] Download finished. Processed {processed}/{total} ({processed_size}), {errors} errors.".format(
                status=status, processed_size=humanize.naturalsize(global_stats["size"], binary=True), **global_stats
            ),
            fg=color,
        )
        return global_stats["errors"] == 0

    def validate(self) -> bool:
        validated_versions_list = ValidatedContentVersionList(self._data_dir)
        if validated_versions_list.data_file_exist():
            validated_versions_list.load_data_from_file()
        global_stats = {
            "total": 0,
            "processed": 0,
            "invalid": 0,
        }
        for archivist_obj in self._objects.values():
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
                download_content_version_list=download_list,
                validated_content_version_list=validated_versions_list,
                max_workers=self._max_workers,
            )
            global_stats["total"] += stats.total
            global_stats["processed"] += stats.processed
            global_stats["invalid"] += stats.invalid
        status = "SUCCESS" if global_stats["invalid"] == 0 else "FAILED"
        color = "green" if global_stats["invalid"] == 0 else "red"
        click.secho(
            "[{status}] Download validation finished. Processed {processed}/{total}, {invalid} errors.".format(
                status=status, **global_stats
            ),
            fg=color,
        )
        return global_stats["invalid"] == 0

    @staticmethod
    def _print_msg(msg: str, obj_type: str, fg: str | None = None) -> None:
        click.secho("[{obj_type}] {msg}".format(obj_type=obj_type, msg=msg), fg=fg)
