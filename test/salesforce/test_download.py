import concurrent.futures
import os
import tempfile
import threading
from unittest.mock import patch, call, Mock, MagicMock

import pytest
from simple_salesforce.api import Usage

from salesforce_archivist.salesforce.api import ApiUsage
from test.salesforce.helper import gen_csv
from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList, ContentDocumentLink
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import (
    DownloadedContentVersion,
    DownloadedContentVersionList,
    DownloadContentVersionList,
    ContentVersionDownloader,
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


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit")
def test_content_version_downloader_download_will_download_in_parallel(submit_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User", config={})
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.data_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
    version_list.add_version(
        version=ContentVersion(
            id="VID1", document_id=link.content_document_id, checksum="c1", extension="ext1", title="version1"
        )
    )
    version_list.add_version(
        version=ContentVersion(
            id="VID2", document_id=link.content_document_id, checksum="c2", extension="ext2", title="version2"
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, archivist_obj=archivist_obj
    )
    downloaded_version_list = DownloadedContentVersionList(data_dir=archivist_obj.data_dir)
    sf_client = Mock()
    downloader = ContentVersionDownloader(
        sf_client=sf_client,
        downloaded_version_list=downloaded_version_list,
    )
    downloader.download(download_list=download_content_version_list)
    assert submit_mock.call_count == 2


@patch("os.path.exists")
def test_content_version_downloader_download_content_version_from_sf_will_add_already_downloaded_version_to_list(
    exist_mock,
):
    exist_mock.return_value = True
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User", config={})
    version = ContentVersion(id="VID", document_id="DID", checksum="c1", extension="ext1", title="version1")
    downloaded_version_list = DownloadedContentVersionList(data_dir=archivist_obj.data_dir)
    sf_client = Mock()
    downloader = ContentVersionDownloader(
        sf_client=sf_client,
        downloaded_version_list=downloaded_version_list,
    )
    downloader.download_content_version_from_sf(version=version, download_path="/fake/path")
    exist_mock.assert_called_once()
    assert len(downloaded_version_list) == 1
    assert downloaded_version_list.get_version(content_version=version).id == version.id


def test_content_version_downloader_download_content_version_from_sf_will_copy_existing_file_to_new_path():
    with tempfile.TemporaryDirectory() as tmpdirname:
        archivist_obj = ArchivistObject(data_dir=tmpdirname, obj_type="User", config={})

        already_downloaded_path = os.path.join(archivist_obj.data_dir, "files", "file1.txt")
        to_download_path = os.path.join(archivist_obj.data_dir, "files", "file2.txt")
        download_list_mock = MagicMock()
        version1 = ContentVersion(id="CID", document_id="DOC1", checksum="c", extension="e", title="title")
        version2 = ContentVersion(id="CID", document_id="DOC2", checksum="c", extension="e", title="title")
        download_list_mock.__iter__.return_value = [
            (version1, already_downloaded_path),
            (version2, to_download_path),
        ]
        os.makedirs(os.path.dirname(already_downloaded_path), exist_ok=True)
        file_contents = b"test"
        with open(already_downloaded_path, "wb") as downloaded_file:
            downloaded_file.write(file_contents)

        downloaded_version_list = DownloadedContentVersionList(data_dir=archivist_obj.data_dir)
        downloaded_version = DownloadedContentVersion(
            id=version1.id,
            document_id=version1.document_id,
            path=already_downloaded_path,
        )
        downloaded_version_list.add_version(downloaded_version)
        sf_client = Mock()
        downloader = ContentVersionDownloader(
            sf_client=sf_client,
            downloaded_version_list=downloaded_version_list,
        )
        downloader.download_content_version_from_sf(version=version2, download_path=to_download_path)
        assert os.path.exists(to_download_path)
        with open(to_download_path, "rb") as new_file:
            assert new_file.read() == file_contents


def test_content_version_downloader_download_content_version_from_sf_will_download_from_salesforce():
    with tempfile.TemporaryDirectory() as tmpdirname:
        archivist_obj = ArchivistObject(data_dir=tmpdirname, obj_type="User", config={})
        version = ContentVersion(id="VID1", document_id="DOC1", checksum="c1", extension="ext1", title="version1")
        downloaded_version_list = DownloadedContentVersionList(data_dir=archivist_obj.data_dir)

        sf_client = MagicMock()
        sf_client.download_content_version.return_value.iter_content.return_value = [b"test"]
        download_list_mock = MagicMock()
        download_list_mock.return_value.__iter__.return_value = []
        downloader = ContentVersionDownloader(
            sf_client=sf_client,
            downloaded_version_list=downloaded_version_list,
        )
        path = os.path.join(tmpdirname, "test.txt")
        downloader.download_content_version_from_sf(version=version, download_path=path)
        assert os.path.exists(path)
        with open(path, "rb") as file:
            assert file.read() == b"test"


@patch("salesforce_archivist.salesforce.download.sleep", spec=True, return_value=None)
def test_content_version_downloader_download_or_wait(sleep_mock):
    sf_client = MagicMock()
    api_usage = ApiUsage(Usage(used=50, total=100))

    def usage_side_effect(refresh: bool) -> ApiUsage:
        nonlocal api_usage
        if refresh:
            api_usage = ApiUsage(Usage(used=10, total=100))
        return api_usage

    sf_client.get_api_usage.side_effect = lambda refresh=False: usage_side_effect(refresh=refresh)
    download_list_mock = MagicMock()
    download_list_mock.__iter__.return_value = []
    with patch.object(ContentVersionDownloader, "download_content_version_from_sf"):
        wait = 7
        downloader = ContentVersionDownloader(
            sf_client=sf_client,
            downloaded_version_list=MagicMock(),
            max_api_usage_percent=30,
            wait_sec=wait,
        )
        downloader.download_or_wait(
            ContentVersion(id="ID", document_id="DOC", checksum="c", extension="e", title="T"),
            download_path="/fake/download/path",
            lock=threading.Lock(),
        )
        sleep_mock.assert_called_once_with(wait)
