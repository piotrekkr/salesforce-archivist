import concurrent.futures
import os
import tempfile
from unittest.mock import patch, call, Mock, MagicMock

import pytest
from simple_salesforce.api import Usage

from salesforce_archivist.salesforce.api import ApiUsage
from salesforce_archivist.salesforce.attachment import Attachment, AttachmentList
from test.salesforce.helper import gen_csv
from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList, ContentDocumentLink
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import (
    DownloadedSalesforceObject,
    DownloadedList,
    DownloadContentVersionList,
    Downloader,
    DownloadStats,
    DownloadAttachmentList,
)


def test_downloaded_salesforce_object_props():
    obj_id, path = ("ID", "/path/to/file.txt")
    downloaded_ver = DownloadedSalesforceObject(obj_id=obj_id, path=path)
    assert (downloaded_ver.id, downloaded_ver.path) == (obj_id, path)


def test_downloaded_salesforce_object_equality():
    obj_id, path = ("ID", "path/to/file.txt")
    sf_obj_1 = DownloadedSalesforceObject(obj_id=obj_id, path=path)
    sf_obj_2 = DownloadedSalesforceObject(obj_id=obj_id, path=path)
    assert sf_obj_1 == sf_obj_2


@patch("os.path.exists")
def test_downloaded_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    download_list = DownloadedList(data_dir=data_dir, file_name="downloaded_versions.csv")
    assert download_list.path == os.path.join(data_dir, "downloaded_versions.csv")
    assert download_list.data_file_exist()
    assert not download_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Id", "Path on disk"],
            ],
        ],
        [
            [
                ["Id", "Path on disk"],
                ["Id_1", "data/path/file_1.txt"],
                ["Id_2", "data/path/file_2.txt"],
            ],
        ],
    ],
)
def test_downloaded_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch.object(DownloadedList, "add") as add_version_mock:
            download_list = DownloadedList(data_dir=tmp_dir, file_name="downloaded_versions.csv")
            gen_csv(data=csv_data, path=download_list.path)
            download_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(call(version=DownloadedSalesforceObject(obj_id=row[0], path=row[1])))
            assert add_version_mock.mock_calls == expected_calls


def test_downloaded_list_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        version_list = DownloadedList(data_dir=tmp_dir, file_name="downloaded_versions.csv")
        to_save = [
            DownloadedSalesforceObject(obj_id="id1", path="data/path/file_2.txt"),
            DownloadedSalesforceObject(obj_id="id2", path="data/path/file_2.txt"),
        ]
        for version in to_save:
            version_list.add(obj=version)
        version_list.save()
        loaded_list = DownloadedList(data_dir=tmp_dir, file_name="downloaded_versions.csv")
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for version in to_save:
            cv = ContentVersion(
                version_id=version.id,
                document_id="DID1",
                title="title",
                checksum="checksum",
                extension="ext",
                version_number=1,
                content_size=10,
            )
            assert version == loaded_list.get(obj=cv)


def test_downloaded_list_add_get():
    downloaded_list = DownloadedList(data_dir="/fake/dir", file_name="downloaded_versions.csv")
    downloaded_sf_object = DownloadedSalesforceObject(obj_id="id1", path="path/file.txt")
    downloaded_list.add(obj=downloaded_sf_object)
    cv = ContentVersion(
        version_id=downloaded_sf_object.id,
        document_id="X",
        title="title",
        checksum="checksum",
        extension="ext",
        version_number=1,
        content_size=10,
    )
    cv2 = ContentVersion(
        version_id="X",
        document_id="Y",
        title="title",
        checksum="checksum2",
        extension="ext2",
        version_number=1,
        content_size=10,
    )
    assert downloaded_list.get(obj=cv) == downloaded_sf_object
    assert downloaded_list.get(obj=cv2) is None


def test_downloaded_list_is_downloaded():
    downloaded_list = DownloadedList(data_dir="/fake/dir", file_name="downloaded_versions.csv")
    downloaded_obj = DownloadedSalesforceObject(obj_id="id1", path="path/file.txt")
    downloaded_list.add(obj=downloaded_obj)
    version = ContentVersion(
        version_id=downloaded_obj.id,
        document_id="X",
        title="t",
        checksum="c",
        extension="e",
        version_number=1,
        content_size=10,
    )
    version2 = ContentVersion(
        version_id="ABC",
        document_id="Y",
        title="t",
        checksum="c",
        extension="e",
        version_number=2,
        content_size=10,
    )
    attachment = Attachment(attachment_id=downloaded_obj.id, parent_id="pid", content_size=10, name="name")
    assert downloaded_list.is_downloaded(version)
    assert not downloaded_list.is_downloaded(version2)
    assert downloaded_list.is_downloaded(attachment)


def test_download_content_version_list():
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    version = ContentVersion(
        version_id="VID",
        document_id=link.content_document_id,
        checksum="c",
        extension="ext",
        title="version",
        version_number=1,
        content_size=10,
    )
    version_list.add_version(version=version)
    download = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    generator = download.__iter__()
    assert next(generator) == (
        version,
        os.path.join(archivist_obj.obj_dir, "files", link.download_dir_name, version.filename),
    )
    with pytest.raises(StopIteration):
        next(generator)


def test_download_attachment_list():
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="Attachment")
    attachment = Attachment(attachment_id="ID", parent_id="PID", content_size=10, name="Name")
    attachment_list = AttachmentList(data_dir=archivist_obj.obj_dir)
    attachment_list.add_attachment(attachment=attachment)
    download = DownloadAttachmentList(attachment_list=attachment_list, data_dir=archivist_obj.obj_dir)
    generator = download.__iter__()
    assert next(generator) == (
        attachment,
        os.path.join(archivist_obj.obj_dir, "files", attachment.parent_id, attachment.filename),
    )
    with pytest.raises(StopIteration):
        next(generator)


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit")
def test_downloader_download_will_download_in_parallel(submit_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    version_list.add_version(
        version=ContentVersion(
            version_id="VID1",
            document_id=link.content_document_id,
            checksum="c1",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
    )
    version_list.add_version(
        version=ContentVersion(
            version_id="VID2",
            document_id=link.content_document_id,
            checksum="c2",
            extension="ext2",
            title="version2",
            version_number=2,
            content_size=10,
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    downloaded_version_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
    sf_client = Mock()
    downloader = Downloader(
        sf_client=sf_client,
    )
    downloader.download(downloaded_list=downloaded_version_list, download_list=download_content_version_list)
    assert submit_mock.call_count == 2


@patch("concurrent.futures.ThreadPoolExecutor")
def test_downloader_download_will_use_defined_workers(thread_pool_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    downloaded_version_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
    sf_client = Mock()
    max_workers = 3
    downloader = Downloader(sf_client=sf_client, max_workers=max_workers)
    downloader.download(downloaded_list=downloaded_version_list, download_list=download_content_version_list)
    assert thread_pool_mock.call_args == call(max_workers=max_workers)


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit", side_effect=KeyboardInterrupt)
@patch.object(concurrent.futures.ThreadPoolExecutor, "shutdown", return_value=None)
def test_downloader_download_will_gracefully_shutdown(shutdown_mock, submit_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    version_list.add_version(
        version=ContentVersion(
            version_id="VID1",
            document_id=link.content_document_id,
            checksum="c1",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    downloaded_version_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
    sf_client = Mock()
    downloader = Downloader(
        sf_client=sf_client,
    )
    with pytest.raises(KeyboardInterrupt):
        downloader.download(downloaded_list=downloaded_version_list, download_list=download_content_version_list)
    shutdown_mock.assert_has_calls([call(wait=True), call(wait=True, cancel_futures=True)])


@patch.object(Downloader, "download_file_from_sf", side_effect=RuntimeError)
def test_downloader_download_will_return_download_stats(download_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    version_list.add_version(
        version=ContentVersion(
            version_id="VID1",
            document_id=link.content_document_id,
            checksum="c1",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    downloaded_version_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
    sf_client = Mock()

    downloader = Downloader(
        sf_client=sf_client,
    )
    result = downloader.download(downloaded_list=downloaded_version_list, download_list=download_content_version_list)
    assert isinstance(result, DownloadStats)
    assert result.total == 1
    assert result.processed == 1
    assert result.errors == 1


@patch("os.path.exists")
def test_downloader_download_file_from_sf_will_add_already_downloaded_object_to_list(
    exist_mock,
):
    exist_mock.return_value = True
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version = ContentVersion(
        version_id="VID",
        document_id="DID",
        checksum="c1",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    attachment = Attachment(attachment_id="ID", parent_id="PID", content_size=10, name="Name")
    downloaded_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
    sf_client = Mock()
    downloader = Downloader(
        sf_client=sf_client,
    )
    downloader.download_file_from_sf(downloaded_list=downloaded_list, download_obj=version, download_path="/fake/path")
    downloader.download_file_from_sf(
        downloaded_list=downloaded_list, download_obj=attachment, download_path="/fake/path"
    )
    assert exist_mock.call_count == 2
    assert len(downloaded_list) == 2
    assert downloaded_list.get(obj=version).id == version.id
    assert downloaded_list.get(obj=attachment).id == attachment.id
    assert sf_client.download_attachment.call_count == 0
    assert sf_client.download_content_version.call_count == 0


def test_downloader_download_file_from_sf_will_copy_existing_file_to_new_path():
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")

        already_downloaded_path = os.path.join(archivist_obj.obj_dir, "files", "file1.txt")
        to_download_path = os.path.join(archivist_obj.obj_dir, "files", "file2.txt")
        download_list_mock = MagicMock()
        obj1 = ContentVersion(
            version_id="CID",
            document_id="DOC1",
            checksum="c",
            extension="e",
            title="title",
            version_number=1,
            content_size=10,
        )
        obj2 = ContentVersion(
            version_id="CID",
            document_id="DOC2",
            checksum="c",
            extension="e",
            title="title",
            version_number=1,
            content_size=10,
        )
        download_list_mock.__iter__.return_value = [
            (obj1, already_downloaded_path),
            (obj2, to_download_path),
        ]
        os.makedirs(os.path.dirname(already_downloaded_path), exist_ok=True)
        file_contents = b"test"
        with open(already_downloaded_path, "wb") as downloaded_file:
            downloaded_file.write(file_contents)

        downloaded_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")
        downloaded_obj = DownloadedSalesforceObject(
            obj_id=obj1.id,
            path=already_downloaded_path,
        )
        downloaded_list.add(downloaded_obj)
        sf_client = Mock()
        downloader = Downloader(
            sf_client=sf_client,
        )
        downloader.download_file_from_sf(
            download_obj=obj2,
            download_path=to_download_path,
            downloaded_list=downloaded_list,
        )
        assert os.path.exists(to_download_path)
        with open(to_download_path, "rb") as new_file:
            assert new_file.read() == file_contents


def test_downloader_download_file_from_sf_will_download_version_from_salesforce():
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        obj = ContentVersion(
            version_id="VID1",
            document_id="DOC1",
            checksum="c1",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
        downloaded_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")

        sf_client = MagicMock()
        sf_client.download_content_version.return_value.iter_content.return_value = [b"test"]
        download_list_mock = MagicMock()
        download_list_mock.return_value.__iter__.return_value = []
        downloader = Downloader(
            sf_client=sf_client,
        )
        path = os.path.join(tmp_dir, "test.txt")
        downloader.download_file_from_sf(
            download_obj=obj,
            download_path=path,
            downloaded_list=downloaded_list,
        )
        assert os.path.exists(path)
        with open(path, "rb") as file:
            assert file.read() == b"test"


def test_downloader_download_file_from_sf_will_download_attachment_from_salesforce():
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="Attachment")
        obj = Attachment(
            attachment_id="ID",
            parent_id="PID",
            content_size=10,
            name="Name",
        )
        downloaded_list = DownloadedList(data_dir=archivist_obj.obj_dir, file_name="downloaded_versions.csv")

        sf_client = MagicMock()
        sf_client.download_attachment.return_value.iter_content.return_value = [b"test"]
        download_list_mock = MagicMock()
        download_list_mock.return_value.__iter__.return_value = []
        downloader = Downloader(
            sf_client=sf_client,
        )
        path = os.path.join(tmp_dir, "test.txt")
        downloader.download_file_from_sf(
            download_obj=obj,
            download_path=path,
            downloaded_list=downloaded_list,
        )
        assert os.path.exists(path)
        with open(path, "rb") as file:
            assert file.read() == b"test"


@patch("salesforce_archivist.salesforce.download.sleep", return_value=None)
def test_downloader_download_or_wait(sleep_mock):
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
    with patch.object(Downloader, "download_file_from_sf"):
        wait = 7
        downloader = Downloader(
            sf_client=sf_client,
            max_api_usage_percent=30,
            wait_sec=wait,
        )
        downloader.download_or_wait(
            download_obj=ContentVersion(
                version_id="ID",
                document_id="DOC",
                checksum="c",
                extension="e",
                title="T",
                version_number=1,
                content_size=10,
            ),
            download_path="/fake/download/path",
            downloaded_list=MagicMock(),
        )
        sleep_mock.assert_has_calls([call(1) for _ in range(wait)])


def test_download_stats_initialize():
    stats = DownloadStats()
    stats.initialize(total=11)
    stats.add_processed(size=10, error=True)
    stats.initialize(total=5)
    assert stats.total == 5
    assert stats.processed == 0
    assert stats.errors == 0


def test_download_stats_add_processed():
    stats = DownloadStats()
    stats.initialize(total=3)
    stats.add_processed(size=10, error=True)
    stats.add_processed(size=11)
    assert stats.total == 3
    assert stats.processed == 2
    assert stats.errors == 1
    assert stats.size == 21
    stats.add_processed(size=5)
    stats.add_processed(size=5)
    assert stats.total == 4
    assert stats.processed == 4


def test_download_stats_combine():
    stats = DownloadStats()
    stats.initialize(total=3)
    stats.add_processed(size=10, error=True)
    stats.add_processed(size=11)
    stats2 = DownloadStats()
    stats2.initialize(total=5)
    stats2.add_processed(size=10)
    stats2.add_processed(size=11)
    stats.combine(stats2)
    assert stats.total == 8
    assert stats.processed == 4
    assert stats.errors == 1
    assert stats.size == 42
