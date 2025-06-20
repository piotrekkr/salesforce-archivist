import datetime
import os.path
import tempfile
import textwrap
from unittest.mock import patch, MagicMock, call, ANY

import pytest
import yaml
from pydantic import ValidationError

from salesforce_archivist.archivist import ArchivistObject, ArchivistAuth, ArchivistConfig, Archivist
from salesforce_archivist.salesforce.download import DownloadedList, DownloadStats
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ValidationStats, ValidatedList


@pytest.mark.parametrize(
    "data_dir, obj_type, modified_date_lt, modified_date_gt, dir_name_field, extra_soql_condition",
    [
        (
            "/fake/dir",
            "User",
            datetime.datetime(year=2023, day=1, month=1),
            datetime.datetime(year=2024, day=1, month=1),
            "DirFieldName",
            "Shipment_Status__c = 'SHIPMENT COMPLETED' OR Shipment_Status__c = 'CANCELLED'",
        ),
        (
            "/fake/dir",
            "User",
            None,
            None,
            None,
            None,
        ),
    ],
)
def test_archivist_object_props(
    data_dir, obj_type, modified_date_lt, modified_date_gt, dir_name_field, extra_soql_condition
):
    archivist_obj = ArchivistObject(
        data_dir=data_dir,
        obj_type=obj_type,
        modified_date_lt=modified_date_lt,
        modified_date_gt=modified_date_gt,
        dir_name_field=dir_name_field,
        extra_soql_condition=extra_soql_condition,
    )
    assert (
        archivist_obj.data_dir,
        archivist_obj.obj_type,
        archivist_obj.modified_date_lt,
        archivist_obj.modified_date_gt,
        archivist_obj.dir_name_field,
        archivist_obj.obj_dir,
        archivist_obj.extra_soql_condition,
    ) == (
        data_dir,
        obj_type,
        modified_date_lt,
        modified_date_gt,
        dir_name_field,
        os.path.join(data_dir, obj_type),
        extra_soql_condition,
    )


def test_archivist_object_equality():
    archivist_obj = ArchivistObject(
        data_dir="data/dir",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
        extra_soql_condition=None,
    )
    archivist_obj_equal = ArchivistObject(
        data_dir="data/dir",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
        extra_soql_condition=None,
    )
    archivist_obj_same_different = ArchivistObject(
        data_dir="data/dir2",
        obj_type="User",
        modified_date_lt=None,
        modified_date_gt=datetime.datetime(year=2024, month=1, day=1),
        dir_name_field=None,
        extra_soql_condition=None,
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
    "yaml_str, expect_exception",
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
    extra_soql_condition: "Shipment_Status__c = 'SHIPMENT COMPLETED' OR Shipment_Status__c = 'CANCELLED'"
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
def test_archivist_config_validation(yaml_str, expect_exception):
    with tempfile.TemporaryDirectory() as tmp_dir:
        data = yaml_str.replace("{data_dir}", tmp_dir)
        if expect_exception:
            with pytest.raises(ValidationError):
                ArchivistConfig(**yaml.safe_load(data))
        else:
            ArchivistConfig(**yaml.safe_load(data))


def test_archivist_config_props():
    with tempfile.TemporaryDirectory() as tmp_dir:
        yaml_str = textwrap.dedent(
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
                extra_soql_condition: "MyField__c = 'value'"
              Booking__c: {{}}
            """
        ).format(data_dir=tmp_dir)

        config = ArchivistConfig(**yaml.safe_load(yaml_str))

        assert config.data_dir == tmp_dir
        assert config.max_api_usage_percent == 40.0
        assert config.modified_date_gt == datetime.datetime(year=2011, month=1, day=1, tzinfo=datetime.timezone.utc)
        assert config.modified_date_lt == datetime.datetime(year=2012, month=1, day=1, tzinfo=datetime.timezone.utc)
        assert isinstance(config.auth, ArchivistAuth)
        assert config.auth.username == "test"
        assert config.auth.instance_url == "https://login.salesforce.com/"
        assert config.auth.consumer_key == "abc"
        assert config.auth.private_key == "test\n"
        archivist_object = config.objects["User"]
        assert archivist_object.obj_type == "User"
        assert archivist_object.data_dir == config.data_dir
        assert archivist_object.obj_dir == os.path.join(config.data_dir, archivist_object.obj_type)
        assert archivist_object.dir_name_field == "LinkedEntity.Username"
        assert archivist_object.extra_soql_condition == "MyField__c = 'value'"
        assert archivist_object.modified_date_gt == datetime.datetime(
            year=2017, month=1, day=1, tzinfo=datetime.timezone.utc
        )
        assert archivist_object.modified_date_lt == datetime.datetime(
            year=2023, month=8, day=1, tzinfo=datetime.timezone.utc
        )
        archivist_object_with_defaults = config.objects["Booking__c"]
        assert archivist_object_with_defaults.modified_date_gt == datetime.datetime(
            year=2011, month=1, day=1, tzinfo=datetime.timezone.utc
        )
        assert archivist_object_with_defaults.modified_date_lt == datetime.datetime(
            year=2012, month=1, day=1, tzinfo=datetime.timezone.utc
        )
        assert archivist_object_with_defaults.dir_name_field is None
        assert archivist_object_with_defaults.extra_soql_condition is None


@patch.object(DownloadedList, "data_file_exist", side_effect=[False, False, True, True])
@patch.object(DownloadedList, "load_data_from_file")
def test_archivist_download_will_load_downloaded_list_if_possible(load_mock, exist_mock):
    archivist = Archivist(data_dir="/fake/dir", objects={}, sf_client=MagicMock())
    archivist.download()
    assert exist_mock.call_count == 2
    load_mock.assert_not_called()
    archivist.download()
    assert exist_mock.call_count == 4
    assert load_mock.call_count == 2


@patch.object(Salesforce, "load_attachment_list")
@patch.object(Salesforce, "load_content_document_link_list")
@patch.object(Salesforce, "load_content_version_list")
@patch.object(Salesforce, "download_files")
def test_archivist_download_will_load_lists_and_call_download_method(
    download_mock, load_version_list_mock, load_doc_link_list_mock, load_attachment_list_mock
):
    download_mock.return_value = DownloadStats()
    objects = {
        "User": ArchivistObject(data_dir="/fake/dir", obj_type="User"),
        "Email": ArchivistObject(data_dir="/fake/dir", obj_type="Email"),
        "Attachment": ArchivistObject(data_dir="/fake/dir", obj_type="Attachment"),
    }
    archivist = Archivist(data_dir="/fake/dir", objects=objects, sf_client=MagicMock())
    archivist.download()
    assert load_doc_link_list_mock.call_count == 2
    assert load_version_list_mock.call_count == 2
    assert load_attachment_list_mock.call_count == 1
    assert download_mock.call_count == 3


@patch.object(Salesforce, "load_attachment_list")
@patch.object(Salesforce, "load_content_document_link_list")
@patch.object(Salesforce, "load_content_version_list")
@patch.object(Salesforce, "download_files")
def test_archivist_download_will_return_correct_bool_value(
    download_mock, load_version_list_mock, load_doc_link_list_mock, load_attachment_list_mock
):
    stats_error = DownloadStats()
    stats_error.initialize(total=1)
    stats_error.add_processed(size=1, error=True)
    stats_ok = DownloadStats()
    for stats, expected_return in [(stats_error, False), (stats_ok, True)]:
        download_mock.return_value = stats
        archivist = Archivist(
            data_dir="/fake/dir",
            objects={
                "User": ArchivistObject(data_dir="/fake/dir", obj_type="User"),
                "Attachment": ArchivistObject(data_dir="/fake/dir", obj_type="Attachment"),
            },
            sf_client=MagicMock(),
        )
        assert archivist.download() == expected_return


@patch.object(ValidatedList, "data_file_exist", side_effect=[False, True])
@patch.object(ValidatedList, "load_data_from_file")
def test_archivist_validate_will_load_validated_list_if_possible(load_mock, exist_mock):
    archivist = Archivist(data_dir="/fake/dir", objects={}, sf_client=MagicMock())
    archivist.validate()
    exist_mock.assert_called_once()
    load_mock.assert_not_called()
    archivist.validate()
    assert exist_mock.call_count == 2
    load_mock.assert_called_once()


@patch.object(Salesforce, "load_attachment_list")
@patch.object(Salesforce, "load_content_document_link_list")
@patch.object(Salesforce, "load_content_version_list")
@patch.object(Salesforce, "validate_download")
def test_archivist_validate_will_load_lists_and_call_validate_method(
    validate_mock, load_version_list_mock, load_doc_link_list_mock, load_attachment_list_mock
):
    validate_mock.return_value = ValidationStats()
    objects = {
        "User": ArchivistObject(data_dir="/fake/dir", obj_type="User"),
        "Email": ArchivistObject(data_dir="/fake/dir", obj_type="Email"),
        "Attachment": ArchivistObject(data_dir="/fake/dir", obj_type="Attachment"),
    }
    max_workers = 6
    archivist = Archivist(data_dir="/fake/dir", objects=objects, sf_client=MagicMock(), max_workers=max_workers)
    archivist.validate()
    assert load_doc_link_list_mock.call_count == 2
    assert load_version_list_mock.call_count == 2
    assert load_attachment_list_mock.call_count == 1
    assert validate_mock.mock_calls == [
        call(download_list=ANY, validated_list=ANY, max_workers=max_workers),
        call(download_list=ANY, validated_list=ANY, max_workers=max_workers),
        call(download_list=ANY, validated_list=ANY, max_workers=max_workers),
    ]
