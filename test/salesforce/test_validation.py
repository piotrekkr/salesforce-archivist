import os
import tempfile
from unittest.mock import patch, call

import pytest
from test.salesforce.helper import gen_csv

from salesforce_archivist.salesforce.validation import (
    ValidatedContentVersion,
    ValidatedContentVersionList,
    ValidationStats,
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
    with tempfile.TemporaryDirectory() as tmpdirname:
        with patch.object(ValidatedContentVersionList, "add_version") as add_version_mock:
            version_list = ValidatedContentVersionList(data_dir=tmpdirname)
            gen_csv(data=csv_data, path=version_list.path)
            version_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(call(version=ValidatedContentVersion(checksum=row[0], path=row[1])))
            assert add_version_mock.mock_calls == expected_calls


def test_validated_content_version_list_save():
    with tempfile.TemporaryDirectory() as tmpdirname:
        version_list = ValidatedContentVersionList(data_dir=tmpdirname)
        to_save = [
            ValidatedContentVersion(checksum="checksum1", path="data/path/file_1.txt"),
            ValidatedContentVersion(checksum="checksum2", path="data/path/file_2.txt"),
        ]
        for version in to_save:
            version_list.add_version(version=version)
        version_list.save()
        loaded_list = ValidatedContentVersionList(data_dir=tmpdirname)
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
