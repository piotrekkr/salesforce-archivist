import os
import tempfile
from unittest.mock import patch, call

import pytest

from test.salesforce.helper import gen_csv
from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList, ContentDocumentLink
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import (
    DownloadedContentVersion,
    DownloadedContentVersionList,
    DownloadContentVersionList,
)


def test_downloaded_content_version_props():
    did, doc_id, path = ("ID", "DOCID", "/path/to/file.txt")
    downloaded_ver = DownloadedContentVersion(id=did, document_id=doc_id, path=path)
    assert (downloaded_ver.id, downloaded_ver.document_id, downloaded_ver.path) == (did, doc_id, path)


def test_downloaded_content_version_equality():
    vid, did, path = ("ID", "DOC_ID", "path/to/file.txt")
    version1 = DownloadedContentVersion(id=vid, document_id=did, path=path)
    version2 = DownloadedContentVersion(id=vid, document_id=did, path=path)
    assert version1 == version2


@patch("os.path.exists")
def test_downloaded_content_version_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    version_list = DownloadedContentVersionList(data_dir=data_dir)
    assert version_list.path == os.path.join(data_dir, "downloaded_versions.csv")
    assert version_list.data_file_exist()
    assert not version_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Id", "ContentDocumentId", "Path on disk"],
            ],
        ],
        [
            [
                ["Id", "ContentDocumentId", "Path on disk"],
                ["Id_1", "ContentDocumentId_1", "data/path/file_1.txt"],
                ["Id_2", "ContentDocumentId_2", "data/path/file_2.txt"],
            ],
        ],
    ],
)
def test_downloaded_content_version_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmpdirname:
        with patch.object(DownloadedContentVersionList, "add_version") as add_version_mock:
            version_list = DownloadedContentVersionList(data_dir=tmpdirname)
            gen_csv(data=csv_data, path=version_list.path)
            version_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(
                    call(version=DownloadedContentVersion(id=row[0], document_id=row[1], path=row[2]))
                )
            assert add_version_mock.mock_calls == expected_calls


def test_downloaded_content_version_list_save():
    with tempfile.TemporaryDirectory() as tmpdirname:
        version_list = DownloadedContentVersionList(data_dir=tmpdirname)
        to_save = [
            DownloadedContentVersion(id="id1", document_id="did1", path="data/path/file_2.txt"),
            DownloadedContentVersion(id="id2", document_id="did2", path="data/path/file_2.txt"),
        ]
        for version in to_save:
            version_list.add_version(version=version)
        version_list.save()
        loaded_list = DownloadedContentVersionList(data_dir=tmpdirname)
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for version in to_save:
            cv = ContentVersion(
                id=version.id, document_id=version.document_id, title="title", checksum="checksum", extension="ext"
            )
            assert version == loaded_list.get_version(content_version=cv)


def test_downloaded_content_version_list_add_get_version():
    version_list = DownloadedContentVersionList(data_dir="/fake/dir")
    version = DownloadedContentVersion(id="id1", document_id="did1", path="path/file.txt")
    version_list.add_version(version=version)
    cv = ContentVersion(
        id=version.id, document_id=version.document_id, title="title", checksum="checksum", extension="ext"
    )
    assert version_list.get_version(content_version=cv) == version
    assert version_list.is_downloaded(content_version=cv)


def test_downloaded_content_version_list_is_downloaded():
    version_list = DownloadedContentVersionList(data_dir="/fake/dir")
    version = DownloadedContentVersion(id="id1", document_id="did1", path="path/file.txt")
    version_list.add_version(version=version)
    cv1 = ContentVersion(id=version.id, document_id=version.document_id, title="t", checksum="c", extension="e")
    cv2 = ContentVersion(id="ABC", document_id=version.document_id, title="t", checksum="c", extension="e")
    assert version_list.is_downloaded(cv1)
    assert not version_list.is_downloaded(cv2)


def test_download_content_version_list():
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User", config={})
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.data_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
    version = ContentVersion(
        id="VID", document_id=link.content_document_id, checksum="c", extension="ext", title="version"
    )
    version_list.add_version(version=version)
    download = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, archivist_obj=archivist_obj
    )
    generator = download.__iter__()
    assert next(generator) == (
        version,
        os.path.join(archivist_obj.data_dir, "files", link.download_dir_name, version.filename),
    )
    with pytest.raises(StopIteration):
        next(generator)
