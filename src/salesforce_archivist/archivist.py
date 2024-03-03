import datetime
import os.path
from typing import Any

import click
import yaml
from schema import And, Optional, Or, Schema, Use
from simple_salesforce import Salesforce as SalesforceClient

from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.download import DownloadContentVersionList, DownloadedContentVersionList
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ValidatedContentVersionList


class ArchivistObject:
    def __init__(
        self,
        data_dir: str,
        obj_type: str,
        modified_date_lt: datetime.datetime | None = None,
        modified_date_gt: datetime.datetime | None = None,
        dir_name_field: str | None = None,
    ):
        self._data_dir: str = os.path.join(data_dir, obj_type)
        self._obj_type: str = obj_type
        self._modified_date_lt: datetime.datetime | None = modified_date_lt
        self._modified_date_gt: datetime.datetime | None = modified_date_gt
        self._dir_name_field: str | None = dir_name_field

    @property
    def data_dir(self) -> str:
        return self._data_dir

    @property
    def obj_type(self) -> str:
        return self._obj_type

    @property
    def modified_date_lt(self) -> datetime.datetime | None:
        return self._modified_date_lt

    @property
    def modified_date_gt(self) -> datetime.datetime | None:
        return self._modified_date_gt

    @property
    def dir_name_field(self) -> str | None:
        return self._dir_name_field

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


class ArchivistAuth:
    def __init__(self, instance_url: str, username: str, consumer_key: str, private_key: str):
        self._instance_url = instance_url
        self._username = username
        self._consumer_key = consumer_key
        self._private_key = private_key

    @property
    def instance_url(self) -> str:
        return self._instance_url

    @property
    def username(self) -> str:
        return self._username

    @property
    def consumer_key(self) -> str:
        return self._consumer_key

    @property
    def private_key(self) -> str:
        return self._private_key


class ArchivistConfig:
    _schema = Schema(
        {
            "data_dir": And(str, len, os.path.isdir, error="data_dir must be set and be a directory"),
            "max_api_usage_percent": Or(int, float, Use(float), lambda v: 0.0 < v <= 100.0),
            Optional("max_workers"): Optional(int, lambda v: 0 < v),
            Optional("modified_date_gt"): lambda d: isinstance(d, datetime.datetime),
            Optional("modified_date_lt"): lambda d: isinstance(d, datetime.datetime),
            "auth": {
                "instance_url": And(str, len),
                "username": And(str, len),
                "consumer_key": And(str, len),
                "private_key": And(bytes, len, Use(lambda b: b.decode("UTF-8"))),
            },
            "objects": {
                str: {
                    Optional("modified_date_gt"): lambda d: isinstance(d, datetime.datetime),
                    Optional("modified_date_lt"): lambda d: isinstance(d, datetime.datetime),
                    Optional("dir_name_field"): And(str, len),
                }
            },
        }
    )

    def __init__(self, path: str):
        with open(path) as file:
            config = self._schema.validate(yaml.load(file, Loader=yaml.FullLoader))
        self._auth: ArchivistAuth = ArchivistAuth(**config["auth"])
        self._data_dir: str = config["data_dir"]
        self._max_api_usage_percent: float = config["max_api_usage_percent"]
        self.modified_date_gt: datetime.datetime | None = config.get("modified_date_gt")
        self.modified_date_lt: datetime.datetime | None = config.get("modified_date_lt")
        self._max_workers: int = config.get("max_workers")
        self._objects = []
        for obj_type, obj_config in config["objects"].items():
            self._objects.append(
                ArchivistObject(
                    data_dir=self._data_dir,
                    obj_type=obj_type,
                    modified_date_lt=obj_config.get("modified_date_lt", self.modified_date_lt),
                    modified_date_gt=obj_config.get("modified_date_gt", self.modified_date_gt),
                    dir_name_field=obj_config.get("dir_name_field"),
                )
            )

    @property
    def data_dir(self) -> str:
        return self._data_dir

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def max_api_usage_percent(self) -> float:
        return self._max_api_usage_percent

    @property
    def auth(self) -> ArchivistAuth:
        return self._auth

    @property
    def objects(self) -> list[ArchivistObject]:
        return self._objects


class Archivist:
    def __init__(
        self,
        data_dir: str,
        objects: list[ArchivistObject],
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
        }
        for archivist_obj in self._objects:
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
                data_dir=archivist_obj.data_dir,
            )
            stats = salesforce.download_files(
                download_content_version_list=download_list,
                downloaded_content_version_list=downloaded_content_versions_list,
                max_workers=self._max_workers,
            )
            global_stats["total"] += stats.total
            global_stats["processed"] += stats.processed
            global_stats["errors"] += stats.errors

        status = "SUCCESS" if global_stats["errors"] == 0 else "FAILED"
        color = "green" if global_stats["errors"] == 0 else "red"
        click.secho(
            "[{status}] Download finished. Processed {processed}/{total}, {errors} errors.".format(
                status=status, **global_stats
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
        for archivist_obj in self._objects:
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
                data_dir=archivist_obj.data_dir,
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
