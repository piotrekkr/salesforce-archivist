import datetime
import os.path

import pytest

from salesforce_archivist.archivist import ArchivistObject, ArchivistAuth


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
    (login_url, username, consumer_key, private_key) = ("http://exmple.com", "username", "consumer_key", "private_key")
    auth = ArchivistAuth(login_url=login_url, username=username, consumer_key=consumer_key, private_key=private_key)
    assert (login_url, username, consumer_key, private_key) == (
        auth.login_url,
        auth.username,
        auth.consumer_key,
        auth.private_key,
    )
