import os.path
import tempfile
from datetime import datetime, timezone
from test.salesforce.helper import gen_temp_csv_files
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest

from salesforce_archivist.archivist import ArchivistObject
from salesforce_archivist.salesforce.api import SalesforceApiClient
from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink, ContentDocumentLinkList
from salesforce_archivist.salesforce.content_version import ContentVersion, ContentVersionList
from salesforce_archivist.salesforce.download import ContentVersionDownloader
from salesforce_archivist.salesforce.salesforce import Salesforce
from salesforce_archivist.salesforce.validation import ContentVersionDownloadValidator


@pytest.mark.parametrize(
    "modified_date_lt, modified_date_gt, dir_name_field, expected_query",
    [
        (
            None,
            None,
            None,
            "SELECT LinkedEntityId, ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntity.Type = 'User'",
        ),
        (
            datetime(
                year=2024,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            ),
            None,
            None,
            (
                "SELECT LinkedEntityId, ContentDocumentId "
                "FROM ContentDocumentLink "
                "WHERE LinkedEntity.Type = 'User' "
                "AND ContentDocument.ContentModifiedDate < 2024-01-01T00:00:00Z"
            ),
        ),
        (
            datetime(
                year=2024,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            ),
            datetime(
                year=2023,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            ),
            None,
            (
                "SELECT LinkedEntityId, ContentDocumentId "
                "FROM ContentDocumentLink "
                "WHERE LinkedEntity.Type = 'User' "
                "AND ContentDocument.ContentModifiedDate < 2024-01-01T00:00:00Z "
                "AND ContentDocument.ContentModifiedDate > 2023-01-01T00:00:00Z"
            ),
        ),
        (
            datetime(
                year=2024,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            ),
            datetime(
                year=2023,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=timezone.utc,
            ),
            "DirField",
            (
                "SELECT LinkedEntityId, ContentDocumentId, DirField "
                "FROM ContentDocumentLink "
                "WHERE LinkedEntity.Type = 'User' "
                "AND ContentDocument.ContentModifiedDate < 2024-01-01T00:00:00Z "
                "AND ContentDocument.ContentModifiedDate > 2023-01-01T00:00:00Z"
            ),
        ),
    ],
)
def test_download_content_document_link_list_queries(
    modified_date_lt: datetime | None,
    modified_date_gt: datetime | None,
    dir_name_field: str | None,
    expected_query: str,
):
    client = Mock()
    client.bulk2 = Mock()
    document_link_list = Mock()
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(
            data_dir=tmp_dir,
            obj_type="User",
            modified_date_lt=modified_date_lt,
            modified_date_gt=modified_date_gt,
            dir_name_field=dir_name_field,
        )
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        salesforce.download_content_document_link_list(
            document_link_list=document_link_list,
        )
        client.bulk2.assert_called_with(
            query=expected_query,
            path=os.path.join(archivist_obj.data_dir, "tmp"),
            max_records=50000,
        )


@pytest.mark.parametrize(
    "csv_files_data",
    [
        # no files
        [],
        # no results from query (file with only header)
        [
            [["LinkedEntityId", "ContentDocumentId"]],
        ],
        # results without custom field for dir name
        [
            [
                ["LinkedEntityId", "ContentDocumentId"],
                ["LinkedEntityId_1", "ContentDocumentId_1"],
                ["LinkedEntityId_2", "ContentDocumentId_2"],
            ]
        ],
        # results with custom field for dir name
        [
            [
                ["LinkedEntityId", "ContentDocumentId", "CustomFieldForDirName"],
                ["LinkedEntityId_1", "ContentDocumentId_1", "CustomFieldForDirName_1"],
                ["LinkedEntityId_2", "ContentDocumentId_2", "CustomFieldForDirName_2"],
            ]
        ],
        # results with custom field for dir name in multiple csv files
        [
            [
                ["LinkedEntityId", "ContentDocumentId", "CustomFieldForDirName"],
                ["LinkedEntityId_1", "ContentDocumentId_1", "CustomFieldForDirName_1"],
                ["LinkedEntityId_2", "ContentDocumentId_2", "CustomFieldForDirName_2"],
            ],
            [
                ["LinkedEntityId", "ContentDocumentId", "CustomFieldForDirName"],
                ["LinkedEntityId_3", "ContentDocumentId_3", "CustomFieldForDirName_3"],
                ["LinkedEntityId_4", "ContentDocumentId_4", "CustomFieldForDirName_4"],
            ],
        ],
    ],
)
def test_download_content_document_link_list_csv_reading(
    csv_files_data: list[list[list[str]]],
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        client = SalesforceApiClient(sf_client=Mock())
        archivist_obj = ArchivistObject(
            data_dir=tmp_dir,
            obj_type="User",
            dir_name_field=(csv_files_data[0][0][2] if len(csv_files_data) and len(csv_files_data[0][0]) > 2 else None),
        )
        client.bulk2 = Mock(
            side_effect=lambda *args, **kwargs: gen_temp_csv_files(
                data=csv_files_data, dir_name=os.path.join(archivist_obj.data_dir, "tmp")
            )
        )
        document_link_list = Mock()
        add_link_calls = []
        for file_data in csv_files_data:
            for row in file_data[1:]:
                doc_link = ContentDocumentLink(
                    linked_entity_id=row[0],
                    content_document_id=row[1],
                    download_dir_name=row[2] if len(row) > 2 else row[0],
                )
                add_link_calls.append(call(doc_link))

        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        salesforce.download_content_document_link_list(
            document_link_list=document_link_list,
        )
        document_link_list.add_link.assert_has_calls(add_link_calls, any_order=True)


@pytest.mark.parametrize(
    "doc_ids, max_records, expected_query, expected_max_records",
    [
        (
            ["DOC_1", "DOC_2"],
            123,
            (
                "SELECT Id, ContentDocumentId, Checksum, Title, FileExtension "
                "FROM ContentVersion "
                "WHERE ContentDocumentId IN ('DOC_1','DOC_2')"
            ),
            123,
        ),
        (
            ["DOC_7", "DOC_1"],
            None,
            (
                "SELECT Id, ContentDocumentId, Checksum, Title, FileExtension "
                "FROM ContentVersion "
                "WHERE ContentDocumentId IN ('DOC_7','DOC_1')"
            ),
            50000,
        ),
    ],
)
def test_download_content_version_list_queries(
    doc_ids: list[str],
    max_records: int | None,
    expected_query: str,
    expected_max_records: int,
):
    client = Mock()
    client.bulk2 = Mock()
    content_version_list = Mock()
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        call_args = {
            "document_ids": doc_ids,
            "content_version_list": content_version_list,
        }
        if max_records is not None:
            call_args["max_records"] = max_records
        salesforce.download_content_version_list(**call_args)
        client.bulk2.assert_called_with(
            query=expected_query,
            path=os.path.join(archivist_obj.data_dir, "tmp"),
            max_records=expected_max_records,
        )


@pytest.mark.parametrize(
    "csv_files_data",
    [
        # no files
        [],
        # no results from query (file with only header)
        [
            [["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension"]],
        ],
        # one file with results
        [
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension"],
                ["Id_1", "ContentDocumentId_1", "Checksum_1", "Title_1", "ext1"],
                ["Id_2", "ContentDocumentId_2", "Checksum_2", "Title_2", "ext2"],
            ],
        ],
        # multiple files with results
        [
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension"],
                ["Id_1", "ContentDocumentId_1", "Checksum_1", "Title_1", "ext1"],
                ["Id_2", "ContentDocumentId_2", "Checksum_2", "Title_2", "ext2"],
            ],
            [
                ["Id", "ContentDocumentId", "Checksum", "Title", "FileExtension"],
                ["Id_3", "ContentDocumentId_3", "Checksum_3", "Title_3", "ext3"],
                ["Id_4", "ContentDocumentId_4", "Checksum_4", "Title_4", "ext4"],
            ],
        ],
    ],
)
def test_download_content_version_list_csv_reading(
    csv_files_data: list[list[list[str]]],
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        client = SalesforceApiClient(sf_client=Mock())
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        client.bulk2 = Mock(
            side_effect=lambda *args, **kwargs: gen_temp_csv_files(
                data=csv_files_data, dir_name=os.path.join(archivist_obj.data_dir, "tmp")
            )
        )
        content_version_list = Mock()
        add_version_calls = []
        for file_data in csv_files_data:
            for row in file_data[1:]:
                version = ContentVersion(
                    id=row[0],
                    document_id=row[1],
                    checksum=row[2],
                    title=row[3],
                    extension=row[4],
                )
                add_version_calls.append(call(version))

        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        salesforce.download_content_version_list(
            document_ids=["DOC_1", "DOC_2"],
            content_version_list=content_version_list,
        )
        content_version_list.add_version.assert_has_calls(add_version_calls, any_order=True)


@patch.object(Salesforce, "download_content_version_list")
@patch.object(ContentVersionList, "data_file_exist", return_value=False)
@patch.object(ContentVersionList, "save", return_value=None)
def test_load_content_version_list_will_call_download_and_save(save_mock, exist_mock, download_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        link_list = []
        doc_ids = []
        for i in range(3):
            link = ContentDocumentLink(
                linked_entity_id="LID{}".format(i),
                content_document_id="DID{}".format(i),
                download_dir_name="LID{}".format(i),
            )
            link_list.append(link)
            doc_ids.append(link.content_document_id)
        doc_link_list = MagicMock()
        doc_link_list.__iter__.return_value = link_list
        client = SalesforceApiClient(sf_client=Mock())
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        ret_val = salesforce.load_content_version_list(document_link_list=doc_link_list, batch_size=10)
        assert isinstance(ret_val, ContentVersionList)
        download_mock.assert_called_once_with(document_ids=doc_ids, content_version_list=ANY)
        save_mock.assert_called_once()


@patch.object(Salesforce, "download_content_version_list")
@patch.object(ContentVersionList, "data_file_exist", return_value=False)
@patch.object(ContentVersionList, "save", return_value=None)
def test_load_content_version_list_will_call_download_in_batches(save_mock, exist_mock, download_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(
            data_dir=tmp_dir,
            obj_type="User",
        )
        link_list = []
        doc_ids = []
        for i in range(3):
            link = ContentDocumentLink(
                linked_entity_id="LID{}".format(i),
                content_document_id="DID{}".format(i),
                download_dir_name="LID{}".format(i),
            )
            link_list.append(link)
            doc_ids.append(link.content_document_id)
        doc_link_list = MagicMock()
        doc_link_list.__iter__.return_value = link_list
        client = SalesforceApiClient(sf_client=Mock())
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        ret_val = salesforce.load_content_version_list(document_link_list=doc_link_list, batch_size=1)
        assert isinstance(ret_val, ContentVersionList)
        download_mock.assert_has_calls(
            calls=[
                call(document_ids=["DID0"], content_version_list=ANY),
                call(document_ids=["DID1"], content_version_list=ANY),
                call(document_ids=["DID2"], content_version_list=ANY),
            ]
        )


@patch.object(Salesforce, "download_content_version_list")
@patch.object(ContentVersionList, "data_file_exist", return_value=True)
@patch.object(ContentVersionList, "load_data_from_file", return_value=None)
@patch.object(ContentVersionList, "save", return_value=None)
def test_load_content_version_list_will_load_from_file(save_mock, load_mock, exist_mock, download_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        doc_link_list = Mock()
        client = SalesforceApiClient(sf_client=Mock())
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        ret_val = salesforce.load_content_version_list(document_link_list=doc_link_list, batch_size=1)
        assert isinstance(ret_val, ContentVersionList)
        load_mock.assert_called_once()
        save_mock.assert_not_called()
        download_mock.assert_not_called()


@patch.object(Salesforce, "download_content_document_link_list")
@patch.object(ContentDocumentLinkList, "data_file_exist", return_value=False)
@patch.object(ContentDocumentLinkList, "save", return_value=None)
def test_load_content_document_link_list_will_call_download_and_save(save_mock, exist_mock, download_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        client = SalesforceApiClient(sf_client=Mock())
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        ret_val = salesforce.load_content_document_link_list()
        assert isinstance(ret_val, ContentDocumentLinkList)
        download_mock.assert_called_once()
        assert isinstance(download_mock.mock_calls[0].kwargs["document_link_list"], ContentDocumentLinkList)
        save_mock.assert_called_once()


@patch.object(Salesforce, "download_content_document_link_list")
@patch.object(ContentDocumentLinkList, "data_file_exist", return_value=True)
@patch.object(ContentDocumentLinkList, "load_data_from_file", return_value=None)
@patch.object(ContentDocumentLinkList, "save", return_value=None)
def test_load_content_document_link_list_will_load_from_file(save_mock, load_mock, exist_mock, download_mock):
    with tempfile.TemporaryDirectory() as tmp_dir:
        archivist_obj = ArchivistObject(data_dir=tmp_dir, obj_type="User")
        client = SalesforceApiClient(sf_client=Mock())
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=50)
        ret_val = salesforce.load_content_document_link_list()
        assert isinstance(ret_val, ContentDocumentLinkList)
        load_mock.assert_called_once()
        download_mock.assert_not_called()
        save_mock.assert_not_called()


def test_download_files_will_call_download_and_save():
    with patch.object(ContentVersionDownloader, "download") as download_mock:
        download_mock.side_effect = [None, Exception("test")]
        archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
        client = SalesforceApiClient(sf_client=Mock())
        max_api_usage = 50
        salesforce = Salesforce(archivist_obj=archivist_obj, client=client, max_api_usage_percent=max_api_usage)
        download_content_version_list = MagicMock()
        download_content_version_list.__iter__.return_value = []
        downloaded_content_version_list = Mock()
        salesforce.download_files(
            download_content_version_list=download_content_version_list,
            downloaded_content_version_list=downloaded_content_version_list,
            max_workers=5,
        )
        with pytest.raises(Exception):
            salesforce.download_files(
                download_content_version_list=download_content_version_list,
                downloaded_content_version_list=downloaded_content_version_list,
                max_workers=5,
            )
        download_mock.assert_has_calls(
            [
                call(download_list=download_content_version_list, max_workers=5),
                call(download_list=download_content_version_list, max_workers=5),
            ]
        )
        assert downloaded_content_version_list.save.call_count == 2


def test_validate_download_will_call_validate_and_save():
    with patch.object(ContentVersionDownloadValidator, "validate") as validate_mock:
        validate_mock.side_effect = [None, Exception("test")]
        archivist_obj = ArchivistObject(data_dir="/fake/dir", obj_type="User")
        salesforce = Salesforce(archivist_obj=archivist_obj, client=Mock(), max_api_usage_percent=50)
        download_content_version_list = MagicMock()
        download_content_version_list.__iter__.return_value = []
        validated_content_version_list = Mock()
        salesforce.validate_download(
            download_content_version_list=download_content_version_list,
            validated_content_version_list=validated_content_version_list,
            max_workers=5,
        )

        with pytest.raises(Exception):
            salesforce.validate_download(
                download_content_version_list=download_content_version_list,
                validated_content_version_list=validated_content_version_list,
            )
        validate_mock.assert_has_calls(
            [
                call(download_list=download_content_version_list, max_workers=5),
                call(download_list=download_content_version_list, max_workers=5),
            ]
        )
        assert validated_content_version_list.save.call_count == 2
