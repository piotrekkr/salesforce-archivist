import concurrent.futures
import hashlib
import os
import tempfile
from unittest.mock import patch, call

import pytest

from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.attachment import Attachment
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLinkList, ContentDocumentLink
from salesforce_archivist.salesforce.content_version import ContentVersionList, ContentVersion
from salesforce_archivist.salesforce.download import DownloadContentVersionList
from test.salesforce.helper import gen_csv

from salesforce_archivist.salesforce.validation import (
    ValidationStats,
    ValidatedList,
    DownloadValidator,
    ValidatedFile,
)


def test_validated_file_props():
    path, checksum, size = ("/path", "checksum", 10)
    validated_file = ValidatedFile(path=path, checksum=checksum, content_size=size)
    assert (validated_file.path, validated_file.checksum, validated_file.content_size) == (path, checksum, size)

    with pytest.raises(ValueError):
        ValidatedFile(path=path, checksum=None, content_size=None)


def test_validated_file_equality():
    path, checksum, size = ("/path", "checksum", 10)
    validated_1 = ValidatedFile(path=path, checksum=checksum, content_size=size)
    validated_2 = ValidatedFile(path=path, checksum=checksum, content_size=size)
    assert validated_1 == validated_2


@patch("os.path.exists")
def test_validated_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    validated_list = ValidatedList(data_dir=data_dir)
    assert validated_list.path == os.path.join(data_dir, "validated_files.csv")
    assert validated_list.data_file_exist()
    assert not validated_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["Checksum", "Content Size", "Path"],
            ],
        ],
        [
            [
                ["Checksum", "Content Size", "Path"],
                ["data/path/file_1.txt", "", "checksum1"],
                ["data/path/file_2.txt", "20", ""],
            ],
        ],
    ],
)
def test_validated_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch.object(ValidatedList, "add") as add_mock:
            validated_list = ValidatedList(data_dir=tmp_dir)
            gen_csv(data=csv_data, path=validated_list.path)
            validated_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                checksum = row[0] if row[0] != "" else None
                size = int(row[1]) if row[1] != "" else None
                expected_calls.append(
                    call(validated_file=ValidatedFile(checksum=checksum, content_size=size, path=row[2]))
                )
            assert add_mock.mock_calls == expected_calls


def test_validated_list_save():
    with tempfile.TemporaryDirectory() as tmp_dir:
        validated_list = ValidatedList(data_dir=tmp_dir)
        to_save = [
            ValidatedFile(checksum="checksum1", path="data/path/file_1.txt", content_size=None),
            ValidatedFile(checksum=None, path="data/path/file_2.txt", content_size=10),
        ]
        for validated_file in to_save:
            validated_list.add(validated_file=validated_file)
        validated_list.save()
        loaded_list = ValidatedList(data_dir=tmp_dir)
        loaded_list.load_data_from_file()
        assert len(loaded_list) == len(to_save)
        for validated_file in to_save:
            assert validated_file == loaded_list.get(path=validated_file.path)


def test_validated_list_add_get_version():
    validated_list = ValidatedList(data_dir="/fake/dir")
    file_1 = ValidatedFile(checksum="checksum1", path="data/path/file_1.txt", content_size=None)
    file_2 = ValidatedFile(checksum=None, path="data/path/file_2.txt", content_size=10)
    validated_list.add(validated_file=file_1)
    assert validated_list.get(path=file_1.path) == file_1
    assert validated_list.get(path=file_2.path) is None


def test_validated_list_is_downloaded():
    validated_list = ValidatedList(data_dir="/fake/dir")
    validated_file_1 = ValidatedFile(path="path/file.txt", checksum="checksum", content_size=None)
    validated_file_2 = ValidatedFile(path="path/file2.txt", checksum=None, content_size=20)
    validated_list.add(validated_file=validated_file_1)
    assert validated_list.is_validated(path=validated_file_1.path)
    assert not validated_list.is_validated(path=validated_file_2.path)


def test_validation_stats_initialize():
    stats = ValidationStats()
    stats.initialize(total=11)
    stats.add_processed(invalid=True)
    stats.initialize(total=5)
    assert stats.total == 5
    assert stats.processed == 0
    assert stats.invalid == 0


def test_validation_stats_add_processed():
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


def test_validation_stats_combine():
    stats = ValidationStats()
    stats.initialize(total=3)
    stats.add_processed(invalid=True)
    stats.add_processed()
    stats2 = ValidationStats()
    stats2.initialize(total=5)
    stats2.add_processed()
    stats2.add_processed()
    stats.combine(stats2)
    assert stats.total == 8
    assert stats.processed == 4
    assert stats.invalid == 1


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit")
def test_download_validator_validate_will_validate_in_parallel(submit_mock):
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
    download_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    validated_version_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validator = DownloadValidator(validated_list=validated_version_list)
    validator.validate(download_list=download_list)
    assert submit_mock.call_count == 2


@patch("concurrent.futures.ThreadPoolExecutor")
def test_download_validator_validate_will_use_defined_workers(thread_pool_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    link_list = ContentDocumentLinkList(data_dir=archivist_obj.obj_dir)
    version_list = ContentVersionList(data_dir=archivist_obj.obj_dir)
    download_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    max_workers = 3
    validator = DownloadValidator(validated_list=validated_list, max_workers=max_workers)
    validator.validate(download_list=download_list)
    assert thread_pool_mock.call_args == call(max_workers=max_workers)


@patch.object(concurrent.futures.ThreadPoolExecutor, "submit", side_effect=KeyboardInterrupt)
@patch.object(concurrent.futures.ThreadPoolExecutor, "shutdown", return_value=None)
def test_download_validator_validate_will_gracefully_shutdown(shutdown_mock, submit_mock):
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
    download_list = DownloadContentVersionList(
        document_link_list=link_list, content_version_list=version_list, data_dir=archivist_obj.obj_dir
    )
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validator = DownloadValidator(validated_list=validated_list)
    with pytest.raises(KeyboardInterrupt):
        validator.validate(download_list=download_list)
    shutdown_mock.assert_has_calls([call(wait=True), call(wait=True, cancel_futures=True)])


@pytest.mark.parametrize(
    "obj_type, object_to_validate",
    [
        (
            "User",
            ContentVersion(
                version_id="VID1",
                document_id="DID",
                checksum="c1",
                extension="ext1",
                title="version1",
                version_number=1,
                content_size=10,
            ),
        ),
        (
            "Attachment",
            Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=10),
        ),
    ],
)
def test_download_validator_validate_object_will_find_missing_file(obj_type, object_to_validate):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type=obj_type)
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validator = DownloadValidator(validated_list=validated_list)
    assert not validator.validate_object(obj=object_to_validate, download_path="/non/existing/path")


@patch("os.path.exists", return_value=True)
def test_download_validator_validate_object_will_check_validated_version_checksum(exists_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version_fail = ContentVersion(
        version_id="VID1",
        document_id="DID",
        checksum="xyz",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    version_ok = ContentVersion(
        version_id="VID2",
        document_id="DID",
        checksum="abc",
        extension="ext1",
        title="version1",
        version_number=2,
        content_size=10,
    )
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validated_path = "/path/to/file"
    validated_list.add(ValidatedFile(path=validated_path, checksum="abc", content_size=None))
    validator = DownloadValidator(validated_list=validated_list)
    assert not validator.validate_object(obj=version_fail, download_path=validated_path)
    assert validator.validate_object(obj=version_ok, download_path=validated_path)


@patch("os.path.exists", return_value=True)
def test_download_validator_validate_object_will_check_validated_attachment_file_size(exists_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="Attachment")
    attachment_fail = Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=20)
    attachment_ok = Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=10)
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validated_path = "/path/to/file"
    validated_list.add(ValidatedFile(path=validated_path, checksum=None, content_size=10))
    validator = DownloadValidator(validated_list=validated_list)
    assert not validator.validate_object(obj=attachment_fail, download_path=validated_path)
    assert validator.validate_object(obj=attachment_ok, download_path=validated_path)


@pytest.mark.parametrize(
    "file_data, checksum, should_match",
    [
        ("test", hashlib.md5("test".encode("utf-8")).hexdigest(), True),
        ("test1", hashlib.md5("test".encode("utf-8")).hexdigest(), False),
    ],
)
def test_download_validator_validate_object_will_calculate_checksum_and_check_version(
    file_data: str, checksum: str, should_match: bool
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        download_path = os.path.join(tmp_dir, "file.txt")
        with open(download_path, "wb") as file:
            file.write(file_data.encode("utf-8"))

        version = ContentVersion(
            version_id="VID1",
            document_id="DID",
            checksum=checksum,
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
        validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
        validator = DownloadValidator(validated_list=validated_list)
        assert validator.validate_object(obj=version, download_path=download_path) == should_match
        assert len(validated_list) == 1


@pytest.mark.parametrize(
    "file_data, size, should_match",
    [
        ("test", len("test"), True),
        ("test1", len("test"), False),
    ],
)
def test_download_validator_validate_object_will_calculate_size_and_check_attachment(
    file_data: str, size: int, should_match: bool
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="Attachment")
        download_path = os.path.join(tmp_dir, "file.txt")
        with open(download_path, "wb") as file:
            file.write(file_data.encode("utf-8"))

        attachment = Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=size)

        validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
        validator = DownloadValidator(validated_list=validated_list)
        assert validator.validate_object(obj=attachment, download_path=download_path) == should_match
        assert len(validated_list) == 1


def test_download_validator_validate_object_will_update_validated_list():
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        download_path_version = os.path.join(tmp_dir, "file_v.txt")
        download_path_attachment = os.path.join(tmp_dir, "file_a.txt")
        data = "test".encode("utf-8")
        data_md5 = hashlib.md5(data).hexdigest()
        data_size = len(data)
        with open(download_path_version, "wb") as file:
            file.write(data)
        with open(download_path_attachment, "wb") as file:
            file.write(data)
        version = ContentVersion(
            version_id="VID1",
            document_id="DID",
            checksum="checksum",
            extension="ext1",
            title="version1",
            version_number=1,
            content_size=10,
        )
        attachment = Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=data_size)
        validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
        validator = DownloadValidator(validated_list=validated_list)
        validator.validate_object(obj=version, download_path=download_path_version)
        validator.validate_object(obj=attachment, download_path=download_path_attachment)
        assert len(validated_list) == 2
        assert validated_list.get(download_path_version).checksum == data_md5
        assert validated_list.get(download_path_attachment).content_size == data_size


@patch("os.path.exists", side_effect=RuntimeError("Test error"))
def test_download_validator_validate_object_will_return_invalid_on_exception(exists_mock):
    archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
    version = ContentVersion(
        version_id="VID1",
        document_id="DID",
        checksum="checksum",
        extension="ext1",
        title="version1",
        version_number=1,
        content_size=10,
    )
    attachment = Attachment(attachment_id="AID", parent_id="PID", name="name", content_size=10)
    validated_list = ValidatedList(data_dir=archivist_obj.obj_dir)
    validator = DownloadValidator(validated_list=validated_list)
    assert not validator.validate_object(obj=version, download_path="/fake/path/download")
    assert not validator.validate_object(obj=attachment, download_path="/fake/path/download")
