import os
import re
import tempfile

from salesforce_archivist.salesforce.attachment import Attachment, AttachmentList
from test.salesforce.helper import gen_csv
from unittest.mock import call, patch

import pytest


def test_attachment_properties():
    attachment_id, parent_id, name, content_size = (
        "ID",
        "PARENT_ID",
        "test",
        10,
    )
    attachment = Attachment(
        attachment_id=attachment_id,
        parent_id=parent_id,
        name=name,
        content_size=content_size,
    )
    assert attachment.id == attachment_id
    assert attachment.parent_id == parent_id
    assert attachment.name == name
    assert attachment.filename == "{id}_{name}".format(
        id=attachment_id,
        name=re.sub(r'[/\\?%*:|"<>]', "-", name),
    )


def test_attachment_equality():
    attachment_id, parent_did, name, content_size = ("ID", "PARENT_ID", "NAME", 10)
    attachment1 = Attachment(
        attachment_id=attachment_id,
        parent_id=parent_did,
        name=name,
        content_size=content_size,
    )
    attachment2 = Attachment(
        attachment_id=attachment_id,
        parent_id=parent_did,
        name=name,
        content_size=content_size,
    )
    assert attachment1 == attachment2


@patch("os.path.exists")
def test_attachment_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    attachment_list = AttachmentList(data_dir=data_dir)
    assert attachment_list.path == os.path.join(data_dir, "attachments.csv")
    assert attachment_list.data_file_exist()
    assert not attachment_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Id", "ParentId", "BodySize", "Name"],
            ],
        ],
        [
            [
                ["Id", "ParentId", "BodySize", "Name"],
                ["Id_1", "ParentId_1", "10", "Name_1"],
                ["Id_2", "ParentId_2", "10", "Name_2"],
            ],
        ],
    ],
)
def test_attachment_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch.object(AttachmentList, "add_attachment") as add_attachment_mock:
            attachment_list = AttachmentList(data_dir=tmp_dir)
            gen_csv(data=csv_data, path=attachment_list.path)
            attachment_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(
                    call(
                        version=Attachment(
                            attachment_id=row[0],
                            parent_id=row[1],
                            content_size=row[2],
                            name=row[3],
                        )
                    )
                )
            assert add_attachment_mock.mock_calls == expected_calls


def test_attachment_list_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        attachment_list = AttachmentList(data_dir=tmp_dir)
        to_save = [
            Attachment(
                attachment_id="id1",
                parent_id="pid1",
                content_size=10,
                name="name1",
            ),
            Attachment(
                attachment_id="id2",
                parent_id="pid2",
                content_size=10,
                name="name2",
            ),
        ]
        for attachment in to_save:
            attachment_list.add_attachment(attachment=attachment)
        attachment_list.save()
        loaded_list = AttachmentList(data_dir=tmp_dir)
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for attachment in to_save:
            assert attachment == loaded_list.get_attachment(attachment.id)


def test_attachment_list_get_attachment():
    attachment_list = AttachmentList(data_dir="/fake/dir")
    attachment = Attachment(
        attachment_id="id1",
        parent_id="pid1",
        content_size=10,
        name="name1",
    )
    attachment_list.add_attachment(attachment=attachment)
    assert attachment_list.get_attachment(attachment_id=attachment.id) == attachment
    assert attachment_list.get_attachment(attachment_id="non-existing-one") is None


def test_attachment_list_add_attachment():
    attachment_list = AttachmentList(data_dir="/fake/dir")
    attachment = Attachment(
        attachment_id="id1",
        parent_id="pid1",
        content_size=10,
        name="name1",
    )
    attachment_list.add_attachment(attachment=attachment)
    assert attachment_list.get_attachment(attachment_id=attachment.id) == attachment


def test_attachment_list_add_attachment_does_not_add_duplicates():
    attachment_list = AttachmentList(data_dir="/fake/dir")
    attachment = Attachment(
        attachment_id="id1",
        parent_id="pid1",
        content_size=10,
        name="name1",
    )
    attachment_list.add_attachment(attachment=attachment)
    attachment_list.add_attachment(attachment=attachment)
    assert len(attachment_list) == 1


def test_attachment_list_get_attachments_for_parent():
    attachment_list = AttachmentList(data_dir="/fake/dir")
    attachment1 = Attachment(
        attachment_id="id1",
        parent_id="pid1",
        content_size=10,
        name="name1",
    )
    attachment2 = Attachment(
        attachment_id="id2",
        parent_id="pid2",
        content_size=10,
        name="name2",
    )
    attachment_list.add_attachment(attachment=attachment1)
    attachment_list.add_attachment(attachment=attachment2)
    gen = attachment_list.get_attachments_for_parent(parent_id=attachment1.parent_id)
    assert attachment1 == next(gen)
    with pytest.raises(StopIteration):
        next(gen)


def test_attachment_list_len():
    attachment_list = AttachmentList(data_dir="/fake/dir")
    attachment1 = Attachment(
        attachment_id="id1",
        parent_id="pid1",
        content_size=10,
        name="name1",
    )
    attachment2 = Attachment(
        attachment_id="id2",
        parent_id="pid2",
        content_size=10,
        name="name2",
    )
    attachment_list.add_attachment(attachment=attachment1)
    attachment_list.add_attachment(attachment=attachment2)
    assert 2 == len(attachment_list)
