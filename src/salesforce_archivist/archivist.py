import datetime
import hashlib
import os.path
import threading
from collections import Counter
from math import ceil
from queue import Empty, Queue
from typing import Any, Generator

import click
import yaml
from click._termui_impl import ProgressBar
from schema import And, Optional, Or, Schema, Use
from simple_salesforce import Salesforce as SalesforceClient

from .content_version import (
    ContentVersion,
    ContentVersionList,
    DownloadedContentVersionList,
    ValidatedContentVersion,
    ValidatedContentVersionList,
)
from .document_link import ContentDocumentLinkList
from .salesforce import Client, Salesforce


class ArchivistObject:
    def __init__(self, data_dir: str, obj_type: str, config: dict[str, Any]):
        self._data_dir: str = os.path.join(data_dir, obj_type)
        self._obj_type: str = obj_type
        self._modified_date_lt: datetime.datetime | None = config.get(
            "modified_date_lt"
        )
        self._modified_date_gt: datetime.datetime | None = config.get(
            "modified_date_gt"
        )
        self._dir_name_field: str | None = config.get("dir_name_field")

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


class ArchivistAuth:
    def __init__(self, config: dict[str, str]):
        self._login_url = config["instance_url"]
        self._username = config["username"]
        self._consumer_key = config["consumer_key"]
        self._private_key = config["private_key"]

    @property
    def login_url(self) -> str:
        return self._login_url

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
            "data_dir": And(
                str, len, os.path.isdir, error="data_dir must be set and be a directory"
            ),
            "max_api_usage_percent": Or(
                int, float, Use(float), lambda v: 0.0 < v <= 100.0
            ),
            "auth": {
                "instance_url": And(str, len),
                "username": And(str, len),
                "consumer_key": And(str, len),
                "private_key": And(bytes, len, Use(lambda b: b.decode("UTF-8"))),
            },
            "objects": {
                str: {
                    # "query": And(str, len),
                    Optional(
                        Or("modified_date_gt", "modified_date_lt")
                    ): lambda d: isinstance(d, datetime.datetime),
                    Optional("dir_name_field"): And(str, len),
                }
            },
        }
    )

    def __init__(self, path: str):
        with open(path) as file:
            config = self._schema.validate(yaml.load(file, Loader=yaml.FullLoader))
        self._auth: ArchivistAuth = ArchivistAuth(config["auth"])
        self._data_dir: str = config["data_dir"]
        self._max_api_usage_percent: float = config["max_api_usage_percent"]
        self._objects = [
            ArchivistObject(self._data_dir, obj_type, config)
            for obj_type, config in config["objects"].items()
        ]

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
    def objects(self) -> Generator:
        yield from self._objects


class Archivist:
    def __init__(self, config: ArchivistConfig):
        self._config = config
        self._sf_client = SalesforceClient(
            instance_url=config.auth.login_url,
            username=config.auth.username,
            consumer_key=config.auth.consumer_key,
            privatekey=config.auth.private_key,
        )
        self._downloaded_version_list = DownloadedContentVersionList(
            self._config.data_dir
        )

    def download(self) -> None:
        for archivist_obj in self._config.objects:
            os.makedirs(archivist_obj.data_dir, exist_ok=True)
            salesforce = Salesforce(
                data_dir=archivist_obj.data_dir,
                client=Client(self._sf_client),
                max_api_usage_percent=self._config.max_api_usage_percent,
            )
            document_link_list = self._load_document_link_list(
                salesforce=salesforce, archivist_obj=archivist_obj
            )
            content_version_list = self._load_content_version_list(
                salesforce=salesforce,
                archivist_obj=archivist_obj,
                document_link_list=document_link_list,
            )
            downloaded_versions_list = DownloadedContentVersionList(
                self._config.data_dir
            )
            self._download_object_files(
                salesforce=salesforce,
                archivist_obj=archivist_obj,
                document_link_list=document_link_list,
                content_version_list=content_version_list,
                downloaded_versions_list=downloaded_versions_list,
            )

    def validate(self) -> None:
        validated_versions_list = ValidatedContentVersionList(self._config.data_dir)
        try:
            for archivist_obj in self._config.objects:
                os.makedirs(archivist_obj.data_dir, exist_ok=True)
                salesforce = Salesforce(
                    data_dir=archivist_obj.data_dir,
                    client=Client(self._sf_client),
                    max_api_usage_percent=self._config.max_api_usage_percent,
                )
                document_link_list = self._load_document_link_list(
                    salesforce=salesforce, archivist_obj=archivist_obj
                )
                content_version_list = self._load_content_version_list(
                    salesforce=salesforce,
                    archivist_obj=archivist_obj,
                    document_link_list=document_link_list,
                )
                self._validate_object_files(
                    archivist_obj=archivist_obj,
                    document_link_list=document_link_list,
                    content_version_list=content_version_list,
                    validated_versions_list=validated_versions_list,
                )
        finally:
            validated_versions_list.save()

    def _validate_object_files(
        self,
        archivist_obj: ArchivistObject,
        document_link_list: ContentDocumentLinkList,
        content_version_list: ContentVersionList,
        validated_versions_list: ValidatedContentVersionList,
        thread_num: int = 3,
    ) -> None:
        queue: Queue = Queue()

        for link in document_link_list.get_links().values():
            for version in content_version_list.get_content_versions_for_link(link):
                path = os.path.join(
                    archivist_obj.data_dir,
                    "files",
                    link.download_dir_name,
                    version.filename,
                )
                queue.put((version, path))
        progressbar: ProgressBar

        results: list[dict[str, int]] = [
            {
                "total": 0,
                "missing": 0,
                "invalid": 0,
            }
            for i in range(thread_num)
        ]
        threads = []

        with click.progressbar(
            length=queue.qsize(),
            label="",
            show_eta=False,
        ) as progressbar:
            for i in range(thread_num):
                results[i] = {
                    "total": 0,
                    "missing": 0,
                    "invalid": 0,
                }
                thread = threading.Thread(
                    target=self._content_version_validator,
                    kwargs={
                        "worker_num": i,
                        "queue": queue,
                        "validated_versions_list": validated_versions_list,
                        "result": results[i],
                        "progressbar": progressbar,
                    },
                    daemon=True,
                )
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

        results_sum: Counter = Counter()
        for result in results:
            results_sum.update(result)
        final_result = dict(results_sum)

        click.echo(
            "Total paths processed: {total}, missing: {missing}, invalid: {invalid}".format(
                **final_result
            )
        )
        click.echo(
            "[{result}] Validation finished.".format(
                result="OK"
                if final_result["missing"] == 0 and final_result["invalid"] == 0
                else "FAILED"
            )
        )

    def _content_version_validator(
        self,
        worker_num: int,
        queue: Queue,
        validated_versions_list: ValidatedContentVersionList,
        result: dict[str, int],
        progressbar: ProgressBar | None = None,
    ) -> None:
        while True:
            try:
                queue_item: tuple[ContentVersion, str] = queue.get_nowait()
            except Empty:
                break

            version, path = queue_item
            try:
                if not os.path.exists(path):
                    click.echo(
                        "[W:{worker}] [ERROR] File does not exist: {path}".format(
                            path=path, worker=worker_num
                        )
                    )
                    result["missing"] += 1
                    continue
                if (
                    validated_version := validated_versions_list.get_version(path)
                ) is not None:
                    if version.checksum != validated_version.checksum:
                        click.echo(
                            "[W:{worker}] [ERROR] File checksum invalid: {path}".format(
                                path=path, worker=worker_num
                            )
                        )
                        result["invalid"] += 1
                        continue
                checksum = self._calculate_md5(path)
                if version.checksum != checksum:
                    click.echo(
                        "[W:{worker}] [ERROR] File checksum invalid: {path}".format(
                            path=path, worker=worker_num
                        )
                    )
                    result["invalid"] += 1
                validated_versions_list.add_version(
                    ValidatedContentVersion(path=path, checksum=checksum)
                )
            except Exception as e:
                click.echo(
                    '[W:{worker}] [ERROR] Exception "{e}" occurred: {path}'.format(
                        path=path, worker=worker_num, e=e
                    )
                )
                result["invalid"] += 1
            finally:
                result["total"] += 1
                queue.task_done()
                if progressbar is not None:
                    progressbar.update(1)

    @staticmethod
    def _calculate_md5(path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            while chunk := f.read(4096):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _load_document_link_list(
        salesforce: Salesforce, archivist_obj: ArchivistObject
    ) -> ContentDocumentLinkList:
        click.echo("Loading document links")
        document_link_list = ContentDocumentLinkList(
            data_dir=archivist_obj.data_dir, dir_name_field=archivist_obj.dir_name_field
        )
        if not os.path.exists(document_link_list.path):
            try:
                click.echo("Fetching link data from Salesforce")
                salesforce.download_content_document_link_list(
                    document_link_list=document_link_list,
                    obj_type=archivist_obj.obj_type,
                    modified_date_gt=archivist_obj.modified_date_gt,
                    modified_date_lt=archivist_obj.modified_date_lt,
                    dir_name_field=archivist_obj.dir_name_field,
                )
            finally:
                document_link_list.save()
                click.echo(
                    "Data saved into {path}".format(path=document_link_list.path)
                )
        click.echo("Document links are loaded!")
        return document_link_list

    def _load_content_version_list(
        self,
        salesforce: Salesforce,
        document_link_list: ContentDocumentLinkList,
        archivist_obj: ArchivistObject,
        batch_size: int = 30,
    ) -> ContentVersionList:
        click.echo("Loading content versions")
        content_version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
        if not os.path.exists(content_version_list.path):
            try:
                doc_id_list = [
                    link.content_document_id
                    for link in document_link_list.get_links().values()
                ]
                list_size = len(doc_id_list)
                all_batches = ceil(list_size / batch_size)
                progressbar: ProgressBar
                with click.progressbar(
                    length=all_batches,
                    label="Fetching content versions",
                    show_eta=False,
                ) as progressbar:
                    self._load_content_version_batches(
                        doc_id_list=doc_id_list,
                        batch_size=batch_size,
                        all_batches=all_batches,
                        salesforce=salesforce,
                        content_version_list=content_version_list,
                        progressbar=progressbar,
                    )
            finally:
                content_version_list.save()
                click.echo(
                    "Data saved into {path}".format(path=content_version_list.path)
                )
        click.echo("Content versions are loaded!")
        return content_version_list

    @staticmethod
    def _load_content_version_batches(
        doc_id_list: list[str],
        batch_size: int,
        all_batches: int,
        salesforce: Salesforce,
        content_version_list: ContentVersionList,
        progressbar: ProgressBar | None = None,
    ) -> None:
        for batch in range(1, all_batches + 1):
            start = (batch - 1) * batch_size
            end = start + batch_size
            doc_id_batch = doc_id_list[start:end]
            salesforce.download_content_version_list(
                document_ids=doc_id_batch,
                content_version_list=content_version_list,
            )
            if progressbar is not None:
                progressbar.update(batch)

    def _download_object_files(
        self,
        salesforce: Salesforce,
        archivist_obj: ArchivistObject,
        document_link_list: ContentDocumentLinkList,
        content_version_list: ContentVersionList,
        downloaded_versions_list: DownloadedContentVersionList,
    ) -> None:
        os.makedirs(os.path.join(archivist_obj.data_dir, "files"), exist_ok=True)
        queue: Queue = Queue()

        for link in document_link_list.get_links().values():
            for version in content_version_list.get_content_versions_for_link(link):
                path = os.path.join(
                    archivist_obj.data_dir,
                    "files",
                    link.download_dir_name,
                    version.filename,
                )
                queue.put((version, path))
        progressbar: ProgressBar
        with click.progressbar(
            length=queue.qsize(),
            label="",
            show_eta=False,
        ) as progressbar:
            try:
                threads = []
                for i in range(3):
                    thread = threading.Thread(
                        target=salesforce.content_version_downloader,
                        kwargs={
                            "worker_num": i,
                            "queue": queue,
                            "downloaded_versions_list": downloaded_versions_list,
                            "max_api_usage_percent": self._config.max_api_usage_percent,
                            "progressbar": progressbar,
                        },
                        daemon=True,
                    )
                    threads.append(thread)
                    thread.start()

                for thread in threads:
                    thread.join()
            finally:
                downloaded_versions_list.save()
