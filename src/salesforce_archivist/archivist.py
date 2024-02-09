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
        # config: dict[str, Any],
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
            "auth": {
                "instance_url": And(str, len),
                "username": And(str, len),
                "consumer_key": And(str, len),
                "private_key": And(bytes, len, Use(lambda b: b.decode("UTF-8"))),
            },
            "objects": {
                str: {
                    Optional(Or("modified_date_gt", "modified_date_lt")): lambda d: isinstance(d, datetime.datetime),
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
        self._objects = []
        for obj_type, config in config["objects"].items():
            self._objects.append(
                ArchivistObject(
                    data_dir=self._data_dir,
                    obj_type=obj_type,
                    modified_date_lt=config.get("modified_date_lt"),
                    modified_date_gt=config.get("modified_date_gt"),
                    dir_name_field=config.get("dir_name_field"),
                )
            )

    @property
    def data_dir(self) -> str:
        return self._data_dir

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
    def __init__(self, config: ArchivistConfig):
        self._config = config
        self._sf_client = SalesforceClient(
            instance_url=config.auth.instance_url,
            username=config.auth.username,
            consumer_key=config.auth.consumer_key,
            privatekey=config.auth.private_key,
        )
        self._downloaded_version_list = DownloadedContentVersionList(self._config.data_dir)
        self._archivist_obj: ArchivistObject | None = None

    def download(self) -> None:
        downloaded_content_versions_list = DownloadedContentVersionList(self._config.data_dir)
        if downloaded_content_versions_list.data_file_exist():
            downloaded_content_versions_list.load_data_from_file()

        global_stats = {
            "total": 0,
            "processed": 0,
            "errors": 0,
        }
        for archivist_obj in self._config.objects:
            obj_type = archivist_obj.obj_type
            salesforce = Salesforce(
                archivist_obj=archivist_obj,
                client=SalesforceApiClient(self._sf_client),
                max_api_usage_percent=self._config.max_api_usage_percent,
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
                archivist_obj=archivist_obj,
            )
            stats = salesforce.download_files(
                download_content_version_list=download_list,
                downloaded_content_version_list=downloaded_content_versions_list,
            )
            global_stats["total"] += stats.total
            global_stats["processed"] += stats.processed
            global_stats["errors"] += stats.errors

        status = "SUCCESS" if global_stats["invalid"] == 0 else "FAILED"
        color = "green" if global_stats["invalid"] == 0 else "red"
        click.secho(
            "[{status}] Download finished. Processed {processed}/{total}, {errors} errors.".format(
                status=status, **global_stats
            ),
            fg=color,
        )

    def validate(self) -> None:
        validated_versions_list = ValidatedContentVersionList(self._config.data_dir)
        if validated_versions_list.data_file_exist():
            validated_versions_list.load_data_from_file()
        global_stats = {
            "total": 0,
            "processed": 0,
            "invalid": 0,
        }
        for archivist_obj in self._config.objects:
            self._archivist_obj = archivist_obj
            salesforce = Salesforce(
                archivist_obj=archivist_obj,
                client=SalesforceApiClient(self._sf_client),
                max_api_usage_percent=self._config.max_api_usage_percent,
            )
            document_link_list = salesforce.load_content_document_link_list()
            content_version_list = salesforce.load_content_version_list(
                document_link_list=document_link_list,
            )
            download_list = DownloadContentVersionList(
                document_link_list=document_link_list,
                content_version_list=content_version_list,
                archivist_obj=archivist_obj,
            )
            stats = salesforce.validate_download(
                download_content_version_list=download_list, validated_content_version_list=validated_versions_list
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

    @staticmethod
    def _print_msg(msg: str, obj_type: str, fg: str | None = None) -> None:
        click.secho("[{obj_type}] {msg}".format(obj_type=obj_type, msg=msg), fg=fg)
