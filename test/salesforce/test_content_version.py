import os
import re
import tempfile
from test.salesforce.helper import gen_csv
from unittest.mock import call, patch

import pytest

from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList


def test_content_version_properties():
    vid, did, title, ext, checksum = ("ID", "DOC_ID", 'TITLE with /\\?%*:|"<> chars', "test", "CHECKSUM")
    version = ContentVersion(id=vid, document_id=did, title=title, extension=ext, checksum=checksum)
    assert version.id == vid
    assert version.document_id == did
    assert version.title == title
    assert version.extension == ext
    assert version.checksum == checksum
    assert version.filename == "{id}_{title}.{extension}".format(
        id=vid, title=re.sub(r'[/\\?%*:|"<>]', "-", title), extension=ext
    )


def test_content_version_equality():
    vid, did, title, ext, checksum = ("ID", "DOC_ID", "TITLE", "test", "CHECKSUM")
    version1 = ContentVersion(id=vid, document_id=did, title=title, extension=ext, checksum=checksum)
    version2 = ContentVersion(id=vid, document_id=did, title=title, extension=ext, checksum=checksum)
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
                ["Id", "ContentDocumentId", "Checksum", "Title", "Extension"],
            ],
        ],
        [
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "Extension"],
                ["Id_1", "ContentDocumentId_1", "Checksum_1", "Title_1", "ext1"],
                ["Id_2", "ContentDocumentId_2", "Checksum_2", "Title_2", "ext2"],
            ],
        ],
    ],
)
def test_content_version_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmpdirname:
        path = os.path.join(tmpdirname, "content_versions.csv")
        gen_csv(data=csv_data, path=path)
        with patch.object(ContentVersionList, "add_version") as add_version_mock:
            version_list = ContentVersionList(data_dir=tmpdirname)
            version_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(
                    call(
                        version=ContentVersion(
                            id=row[0], document_id=row[1], checksum=row[2], title=row[3], extension=row[4]
                        )
                    )
                )
            assert add_version_mock.mock_calls == expected_calls
