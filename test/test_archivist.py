import datetime
import os.path
import tempfile
import textwrap
from unittest.mock import patch, MagicMock, call, ANY

import pytest
import schema

from salesforce_archivist.archivist import ArchivistObject, ArchivistAuth, ArchivistConfig, Archivist
from salesforce_archivist.salesforce.download import DownloadedContentVersionList, DownloadStats
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ValidatedContentVersionList, ValidationStats


@pytest.mark.parametrize(
    "data_dir, obj_type, modified_date_lt, modified_date_gt, dir_name_field",
    [
        (
            "/fake/dir",
            "User",
            datetime.datetime(year=2023, day=1, month=1),
            datetime.datetime(year=2024, day=1, month=1),
            "DirFieldName",
        ),
        (
            "/fake/dir",
            "User",
            None,
            None,
            None,
        ),
    ],
)
def test_archivist_object_props(data_dir, obj_type, modified_date_lt, modified_date_gt, dir_name_field):
    archivist_obj = ArchivistObject(
        data_dir=data_dir,
        obj_type=obj_type,
        modified_date_lt=modified_date_lt,
        modified_date_gt=modified_date_gt,
        dir_name_field=dir_name_field,
    )
    assert (
        archivist_obj.data_dir,
        archivist_obj.obj_type,
        archivist_obj.modified_date_lt,
        archivist_obj.modified_date_gt,
        archivist_obj.dir_name_field,
    ) == (os.path.join(data_dir, obj_type), obj_type, modified_date_lt, modified_date_gt, dir_name_field)


def test_archivist_object_quality():
    archivist_obj = ArchivistObject(
        data_dir="data/dir",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
    )
    archivist_obj_equal = ArchivistObject(
        data_dir="data/dir",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
    )
    archivist_obj_same_different = ArchivistObject(
        data_dir="data/dir2",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
    )
    assert archivist_obj == archivist_obj_equal
    assert archivist_obj != archivist_obj_same_different


def test_archivist_auth_props():
    (instance_url, username, consumer_key, private_key) = (
        "http://exmple.com",
        "username",
        "consumer_key",
        "private_key",
    )
    auth = ArchivistAuth(
        instance_url=instance_url, username=username, consumer_key=consumer_key, private_key=private_key
    )
    assert (instance_url, username, consumer_key, private_key) == (
        auth.instance_url,
        auth.username,
        auth.consumer_key,
        auth.private_key,
    )


@pytest.mark.parametrize(
    "yaml_data, expect_exception",
    [
        (
            """\
data_dir: {data_dir}
max_api_usage_percent: 50
auth:
  instance_url: https://login.salesforce.com/
  username: test
  consumer_key: abc
  private_key: !!binary |
    dGVzdAo=

objects:
  User:
    modified_date_gt: 2017-01-01T00:00:00Z
    modified_date_lt: 2023-08-01T00:00:00Z
    dir_name_field: LinkedEntity.Username
""",
            False,
        ),
        (
            """\
data_dir: {data_dir}
max_api_usage_percent: 50
auth:
  instance_url: https://login.salesforce.com/
  username: test
  consumer_key: abc
  private_key: !!binary |
    dGVzdAo=

objects:
  User: {}
""",
            False,
        ),
        (
            """\
data_dir: {data_dir}
max_api_usage_percent: 50
auth:
  instance_url: https://login.salesforce.com/
  consumer_key: abc
  private_key: !!binary |
    dGVzdAo=

objects:
  User: {}
""",
            True,
        ),
    ],
)
def test_archivist_config_validation(yaml_data, expect_exception):
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = os.path.join(tmp_dir, "config.yaml")
        with open(path, "wb") as config:
            data = yaml_data.replace("{data_dir}", tmp_dir)
            config.write(data.encode("utf-8"))
        if expect_exception:
            with pytest.raises(schema.SchemaError):
                ArchivistConfig(path)
        else:
            ArchivistConfig(path)


def test_archivist_config_props():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yaml = textwrap.dedent(
            """\
            data_dir: {data_dir}
            max_api_usage_percent: 40
            modified_date_gt: 2011-01-01T00:00:00Z
            modified_date_lt: 2012-01-01T00:00:00Z
            auth:
              instance_url: https://login.salesforce.com/
              username: test
              consumer_key: abc
              private_key: !!binary |
                dGVzdAo=

            objects:
              User:
                modified_date_gt: 2017-01-01T00:00:00Z
                modified_date_lt: 2023-08-01T00:00:00Z
                dir_name_field: LinkedEntity.Username
              Booking__c: {{}}
            """
        ).format(data_dir=tmp_dir)
        path = os.path.join(tmp_dir, "config.yaml")
        with open(path, "wb") as config:
            config.write(yaml.encode("utf-8"))

        config = ArchivistConfig(path)

        assert config.data_dir == tmp_dir
        assert config.max_api_usage_percent == 40.0
        assert config.modified_date_gt == datetime.datetime(year=2011, month=1, day=1, tzinfo=datetime.timezone.utc)
        assert config.modified_date_lt == datetime.datetime(year=2012, month=1, day=1, tzinfo=datetime.timezone.utc)
        assert isinstance(config.auth, ArchivistAuth)
        assert config.auth.username == "test"
        assert config.auth.instance_url == "https://login.salesforce.com/"
        assert config.auth.consumer_key == "abc"
        assert config.auth.private_key == "test\n"
        archivist_object = config.objects[0]
        assert archivist_object.obj_type == "User"
        assert archivist_object.data_dir == os.path.join(config.data_dir, archivist_object.obj_type)
        assert archivist_object.dir_name_field == "LinkedEntity.Username"
        assert archivist_object.modified_date_gt == datetime.datetime(
            year=2017, month=1, day=1, tzinfo=datetime.timezone.utc
        )
        assert archivist_object.modified_date_lt == datetime.datetime(
            year=2023, month=8, day=1, tzinfo=datetime.timezone.utc
        )
        archivist_object_with_defaults = config.objects[1]
        assert archivist_object_with_defaults.modified_date_gt == datetime.datetime(
            year=2011, month=1, day=1, tzinfo=datetime.timezone.utc
        )
        assert archivist_object_with_defaults.modified_date_lt == datetime.datetime(
            year=2012, month=1, day=1, tzinfo=datetime.timezone.utc
        )


@patch.object(DownloadedContentVersionList, "data_file_exist", side_effect=[False, True])
@patch.object(DownloadedContentVersionList, "load_data_from_file")
def test_archivist_download_will_load_downloaded_list_if_possible(load_mock, exist_mock):
    archivist = Archivist(data_dir="/fake/dir", objects=[], sf_client=MagicMock())
    archivist.download()
    exist_mock.assert_called_once()
    load_mock.assert_not_called()
    archivist.download()
    assert exist_mock.call_count == 2
    load_mock.assert_called_once()


@patch.object(Salesforce, "load_content_document_link_list")
@patch.object(Salesforce, "load_content_version_list")
@patch.object(Salesforce, "download_files")
def test_archivist_download_will_load_lists_and_call_download_method(
    download_mock, load_version_list_mock, load_doc_link_list_mock
):
    download_mock.return_value = DownloadStats()
    objects = [
        ArchivistObject(data_dir="/fakse/dir", obj_type="User"),
        ArchivistObject(data_dir="/fakse/dir", obj_type="Email"),
    ]
    archivist = Archivist(data_dir="/fake/dir", objects=objects, sf_client=MagicMock())
    archivist.download()
    assert load_doc_link_list_mock.call_count == 2
    assert load_version_list_mock.call_count == 2
    assert download_mock.call_count == 2


@patch.object(ValidatedContentVersionList, "data_file_exist", side_effect=[False, True])
@patch.object(ValidatedContentVersionList, "load_data_from_file")
def test_archivist_validate_will_load_validated_list_if_possible(load_mock, exist_mock):
    archivist = Archivist(data_dir="/fake/dir", objects=[], sf_client=MagicMock())
    archivist.validate()
    exist_mock.assert_called_once()
    load_mock.assert_not_called()
    archivist.validate()
    assert exist_mock.call_count == 2
    load_mock.assert_called_once()


@patch.object(Salesforce, "load_content_document_link_list")
@patch.object(Salesforce, "load_content_version_list")
@patch.object(Salesforce, "validate_download")
def test_archivist_validate_will_load_lists_and_call_validate_method(
    validate_mock, load_version_list_mock, load_doc_link_list_mock
):
    validate_mock.return_value = ValidationStats()
    objects = [
        ArchivistObject(data_dir="/fakse/dir", obj_type="User"),
        ArchivistObject(data_dir="/fakse/dir", obj_type="Email"),
    ]
    max_workers = 6
    archivist = Archivist(data_dir="/fake/dir", objects=objects, sf_client=MagicMock(), max_workers=max_workers)
    archivist.validate()
    assert load_doc_link_list_mock.call_count == 2
    assert load_version_list_mock.call_count == 2
    assert validate_mock.mock_calls == [
        call(download_content_version_list=ANY, validated_content_version_list=ANY, max_workers=max_workers),
        call(download_content_version_list=ANY, validated_content_version_list=ANY, max_workers=max_workers),
    ]
