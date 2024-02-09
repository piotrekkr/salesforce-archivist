import datetime
import os.path
import tempfile
import textwrap

import pytest
import schema

from salesforce_archivist.archivist import ArchivistObject, ArchivistAuth, ArchivistConfig


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
        path = os.path.join(tmp_dir, "config.yml")
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
            dir_name_field: LinkedEntity.Username"""
        ).format(data_dir=tmp_dir)
        path = "config.yml"
        with open(path, "wb") as config:
            config.write(yaml.encode("utf-8"))

        config = ArchivistConfig(path)

        assert config.data_dir == tmp_dir
        assert config.max_api_usage_percent == 40.0
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
