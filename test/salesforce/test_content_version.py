import os
import re
import tempfile

from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink
from test.salesforce.helper import gen_csv
from unittest.mock import call, patch

import pytest

from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList


def test_content_version_properties():
    vid, did, title, ext, checksum, version_number = (
        "ID",
        "DOC_ID",
        'TITLE with /\\?%*:|"<> chars',
        "test",
        "CHECKSUM",
        1,
    )
    version = ContentVersion(
        id=vid, document_id=did, title=title, extension=ext, checksum=checksum, version_number=version_number
    )
    assert version.id == vid
    assert version.document_id == did
    assert version.title == title
    assert version.extension == ext
    assert version.checksum == checksum
    assert version.filename == "{doc_id}_{version_number}_{id}_{title}.{extension}".format(
        id=vid, doc_id=did, title=re.sub(r'[/\\?%*:|"<>]', "-", title), extension=ext, version_number=version_number
    )


def test_content_version_equality():
    vid, did, title, ext, checksum, version_number = ("ID", "DOC_ID", "TITLE", "test", "CHECKSUM", 1)
    version1 = ContentVersion(
        id=vid, document_id=did, title=title, extension=ext, checksum=checksum, version_number=version_number
    )
    version2 = ContentVersion(
        id=vid, document_id=did, title=title, extension=ext, checksum=checksum, version_number=version_number
    )
    assert version1 == version2


@patch("os.path.exists")
def test_content_version_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    version_list = ContentVersionList(data_dir=data_dir)
    assert version_list.path == os.path.join(data_dir, "content_versions.csv")
    assert version_list.data_file_exist()
    assert not version_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension", "VersionNumber"],
            ],
        ],
        [
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension", "VersionNumber"],
                ["Id_1", "ContentDocumentId_1", "Checksum_1", "Title_1", "ext1", "1"],
                ["Id_2", "ContentDocumentId_2", "Checksum_2", "Title_2", "ext2", "1"],
            ],
        ],
    ],
)
def test_content_version_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch.object(ContentVersionList, "add_version") as add_version_mock:
            version_list = ContentVersionList(data_dir=tmp_dir)
            gen_csv(data=csv_data, path=version_list.path)
            version_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(
                    call(
                        version=ContentVersion(
                            id=row[0],
                            document_id=row[1],
                            checksum=row[2],
                            title=row[3],
                            extension=row[4],
                            version_number=row[5],
                        )
                    )
                )
            assert add_version_mock.mock_calls == expected_calls


def test_content_version_list_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        version_list = ContentVersionList(data_dir=tmp_dir)
        to_save = [
            ContentVersion(
                id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
            ),
            ContentVersion(
                id="id2", document_id="did2", checksum="sum2", title="title2", extension="ext2", version_number=1
            ),
        ]
        for version in to_save:
            version_list.add_version(version=version)
        version_list.save()
        loaded_list = ContentVersionList(data_dir=tmp_dir)
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for version in to_save:
            assert version == loaded_list.get_content_version(version.id)


def test_content_version_list_get_content_version():
    version_list = ContentVersionList(data_dir="/fake/dir")
    version = ContentVersion(
        id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
    )
    version_list.add_version(version=version)
    assert version_list.get_content_version(version_id=version.id) == version
    assert version_list.get_content_version(version_id="non-existing-one") is None


def test_content_version_list_add_version():
    version_list = ContentVersionList(data_dir="/fake/dir")
    version = ContentVersion(
        id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
    )
    version_list.add_version(version=version)
    assert version_list.get_content_version(version_id=version.id) == version


def test_content_version_list_add_version_does_not_add_duplicates():
    version_list = ContentVersionList(data_dir="/fake/dir")
    version = ContentVersion(
        id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
    )
    doc_link = ContentDocumentLink(content_document_id=version.document_id, linked_entity_id="LID1")
    version_list.add_version(version=version)
    version_list.add_version(version=version)
    versions_for_doc = [v for v in version_list.get_content_versions_for_link(link=doc_link)]
    assert len(version_list) == 1
    assert len(versions_for_doc) == 1


def test_content_version_list_get_content_versions_for_link():
    version_list = ContentVersionList(data_dir="/fake/dir")
    version1 = ContentVersion(
        id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
    )
    version2 = ContentVersion(
        id="id2", document_id="did2", checksum="sum2", title="title2", extension="ext2", version_number=1
    )
    version_list.add_version(version=version1)
    version_list.add_version(version=version2)
    doc_link = ContentDocumentLink(content_document_id=version1.document_id, linked_entity_id="LID1")
    gen = version_list.get_content_versions_for_link(link=doc_link)
    assert version1 == next(gen)
    with pytest.raises(StopIteration):
        next(gen)


def test_content_version_list_len():
    version_list = ContentVersionList(data_dir="/fake/dir")
    version1 = ContentVersion(
        id="id1", document_id="did1", checksum="sum1", title="title1", extension="ext1", version_number=1
    )
    version2 = ContentVersion(
        id="id2", document_id="did2", checksum="sum2", title="title2", extension="ext2", version_number=1
    )
    version_list.add_version(version=version1)
    version_list.add_version(version=version2)
    assert 2 == len(version_list)
