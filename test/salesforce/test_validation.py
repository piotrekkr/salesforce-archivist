import concurrent.futures
import hashlib
import os
import tempfile
from unittest.mock import patch, call

import pytest

from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList, ContentDocumentLink
from salesforce_archivist.salesforce.content_version import ContentVersionList, ContentVersion
from salesforce_archivist.salesforce.download import DownloadContentVersionList
from test.salesforce.helper import gen_csv

from salesforce_archivist.salesforce.validation import (
    ValidatedContentVersion,
    ValidatedContentVersionList,
    ValidationStats,
    ContentVersionDownloadValidator,
)


def test_validated_content_version_props():
    path, checksum = ("/path", "checksum")
    validated_ver = ValidatedContentVersion(path=path, checksum=checksum)
    assert (validated_ver.path, validated_ver.checksum) == (path, checksum)


def test_validated_content_version_equality():
    path, checksum = ("/path", "checksum")
    version1 = ValidatedContentVersion(path=path, checksum=checksum)
    version2 = ValidatedContentVersion(path=path, checksum=checksum)
    assert version1 == version2


@patch("os.path.exists")
def test_validated_content_version_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    version_list = ValidatedContentVersionList(data_dir=data_dir)
    assert version_list.path == os.path.join(data_dir, "validated_versions.csv")
    assert version_list.data_file_exist()
    assert not version_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Checksum", "Path"],
            ],
        ],
        [
            [
                ["Checksum", "Path"],
                ["data/path/file_1.txt", "checksum1"],
                ["data/path/file_2.txt", "checksum2"],
            ],
        ],
    ],
)
def test_validated_content_version_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch.object(ValidatedContentVersionList, "add_version") as add_version_mock:
            version_list = ValidatedContentVersionList(data_dir=tmp_dir)
            gen_csv(data=csv_data, path=version_list.path)
            version_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(call(version=ValidatedContentVersion(checksum=row[0], path=row[1])))
            assert add_version_mock.mock_calls == expected_calls


def test_validated_content_version_list_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        version_list = ValidatedContentVersionList(data_dir=tmp_dir)
        to_save = [
            ValidatedContentVersion(checksum="checksum1", path="data/path/file_1.txt"),
            ValidatedContentVersion(checksum="checksum2", path="data/path/file_2.txt"),
        ]
        for version in to_save:
            version_list.add_version(version=version)
        version_list.save()
        loaded_list = ValidatedContentVersionList(data_dir=tmp_dir)
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for version in to_save:
            assert version == loaded_list.get_version(path=version.path)


def test_validated_content_version_list_add_get_version():
    version_list = ValidatedContentVersionList(data_dir="/fake/dir")
    version = ValidatedContentVersion(checksum="checksum1", path="data/path/file_1.txt")
    version2 = ValidatedContentVersion(checksum="checksum2", path="data/path/file_2.txt")
    version_list.add_version(version=version)
    assert version_list.get_version(path=version.path) == version
    assert version_list.get_version(path=version2.path) is None


def test_downloaded_content_version_list_is_downloaded():
    version_list = ValidatedContentVersionList(data_dir="/fake/dir")
    version = ValidatedContentVersion(path="path/file.txt", checksum="checksum")
    version2 = ValidatedContentVersion(path="path/file2.txt", checksum="checksum2")
    version_list.add_version(version=version)
    assert version_list.is_validated(path=version.path)
    assert not version_list.is_validated(path=version2.path)


def test_validation_stats_initialize():
    stats = ValidationStats()
    stats.initialize(total=11)
    stats.add_processed(invalid=True)
    stats.initialize(total=5)
    assert stats.total == 5
    assert stats.processed == 0
    assert stats.invalid == 0


def test_download_stats_add_processed():
    stats = ValidationStats()
    stats.initialize(total=3)
    stats.add_processed(invalid=True)
    stats.add_processed()
    assert stats.total == 3
    assert stats.processed == 2
    assert stats.invalid == 1
    stats.add_processed()
    stats.add_processed()
    assert stats.total == 4
    assert stats.processed == 4


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit")
def test_content_version_download_validator_validate_will_validate_in_parallel(submit_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.data_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
    version_list.add_version(
        version=ContentVersion(
            id="VID1",
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
            id="VID2",
            document_id=link.content_document_id,
            checksum="c2",
            extension="ext2",
            title="version2",
            version_number=2,
            content_size=10,
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.data_dir
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
    validator.validate(download_list=download_content_version_list)
    assert submit_mock.call_count == 2


@patch("concurrent.futures.ThreadPoolExecutor")
def test_content_version_download_validator_validate_will_use_defined_workers(thread_pool_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.data_dir)
    version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.data_dir
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    max_workers = 3
    validator = ContentVersionDownloadValidator(
        validated_content_version_list=validated_version_list, max_workers=max_workers
    )
    validator.validate(download_list=download_content_version_list)
    assert thread_pool_mock.call_args == call(max_workers=max_workers)


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit", side_effect=KeyboardInterrupt)
@patch.object(concurrent.futures.ThreadPoolExecutor, "shutdown", return_value=None)
def test_content_version_download_validator_validate_will_gracefully_shutdown(shutdown_mock, submit_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.data_dir)
    link = ContentDocumentLink(linked_entity_id="LID", content_document_id="DOC1")
    link_list.add_link(doc_link=link)
    version_list = ContentVersionList(data_dir=archivist_obj.data_dir)
    version_list.add_version(
        version=ContentVersion(
            id="VID1",
            document_id=link.content_document_id,
            checksum="c1",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
    )
    download_content_version_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.data_dir
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
    with pytest.raises(KeyboardInterrupt):
        validator.validate(download_list=download_content_version_list)
    shutdown_mock.assert_has_calls([call(wait=True), call(wait=True, cancel_futures=True)])


def test_content_version_download_validator_validate_version_will_find_missing_file():
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version = ContentVersion(
        id="VID1",
        document_id="DID",
        checksum="c1",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
    assert not validator.validate_version(version=version, download_path="/non/existing/path")


@patch("os.path.exists", return_value=True)
def test_content_version_download_validator_validate_version_will_check_validated_checksum(exists_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version_fail = ContentVersion(
        id="VID1",
        document_id="DID",
        checksum="xyz",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    version_ok = ContentVersion(
        id="VID2",
        document_id="DID",
        checksum="abc",
        extension="ext1",
        title="version1",
        version_number=2,
        content_size=10,
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    validated_path = "/path/to/file"
    validated_version_list.add_version(ValidatedContentVersion(path=validated_path, checksum="abc"))
    validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
    assert not validator.validate_version(version=version_fail, download_path=validated_path)
    assert validator.validate_version(version=version_ok, download_path=validated_path)


@pytest.mark.parametrize(
    "file_data, checksum, should_match",
    [
        ("test", hashlib.md5("test".encode("utf-8")).hexdigest(), True),
        ("test1", hashlib.md5("test".encode("utf-8")).hexdigest(), False),
    ],
)
def test_content_version_download_validator_validate_version_will_calculate_checksum_and_check(
    file_data: str, checksum: str, should_match: bool
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        download_path = os.path.join(tmp_dir, "file.txt")
        with open(download_path, "wb") as file:
            file.write(file_data.encode("utf-8"))

        version = ContentVersion(
            id="VID1",
            document_id="DID",
            checksum=checksum,
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
        validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
        validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
        assert validator.validate_version(version=version, download_path=download_path) == should_match
        assert len(validated_version_list) == 1


def test_content_version_download_validator_validate_version_will_update_validated_list():
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        download_path = os.path.join(tmp_dir, "file.txt")
        data = "test".encode("utf-8")
        data_md5 = hashlib.md5(data).hexdigest()
        with open(download_path, "wb") as file:
            file.write(data)
        version = ContentVersion(
            id="VID1",
            document_id="DID",
            checksum="checksum",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
        validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
        validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
        validator.validate_version(version=version, download_path=download_path)
        assert len(validated_version_list) == 1
        assert validated_version_list.get_version(download_path).checksum == data_md5


@patch("os.path.exists", side_effect=RuntimeError("Test error"))
def test_content_version_download_validator_validate_version_will_return_invalid_on_exception(exists_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version = ContentVersion(
        id="VID1",
        document_id="DID",
        checksum="checksum",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    validated_version_list = ValidatedContentVersionList(data_dir=archivist_obj.data_dir)
    validator = ContentVersionDownloadValidator(validated_content_version_list=validated_version_list)
    assert not validator.validate_version(version=version, download_path="/fake/path/download")
