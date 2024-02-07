import datetime
import os.path

import pytest

from salesforce_archivist.archivist import ArchivistObject


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
