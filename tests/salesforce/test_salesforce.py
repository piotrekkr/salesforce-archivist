import csv
import os.path
import tempfile
from datetime import datetime, timezone

from unittest.mock import Mock, call, patch

import pytest

from salesforce_archivist.document_link import ContentDocumentLink
from salesforce_archivist.salesforce import Salesforce, Client

modified_date_lt = datetime(year=2024, month=1, day=1, hour=0, minute=0, second=0, microsecond=0,
                            tzinfo=timezone.utc)
modified_date_gt = datetime(year=2023, month=1, day=1, hour=0, minute=0, second=0, microsecond=0,
                            tzinfo=timezone.utc)
dir_name_field = 'DirField'

query_test_params = [
    (
        {},
        (
            'SELECT LinkedEntityId, ContentDocumentId '
            'FROM ContentDocumentLink '
            "WHERE LinkedEntity.Type = 'User'"
        )
    ),
    (
        {
            'modified_date_lt': modified_date_lt
        },
        (
            'SELECT LinkedEntityId, ContentDocumentId '
            'FROM ContentDocumentLink '
            "WHERE LinkedEntity.Type = 'User' "
            'AND ContentDocument.ContentModifiedDate < {date_lt}'
        ).format(date_lt=modified_date_lt.strftime('%Y-%m-%dT%H:%M:%SZ'))
    ),
    (
        {
            'modified_date_lt': modified_date_lt,
            'modified_date_gt': modified_date_gt
        },
        (
            'SELECT LinkedEntityId, ContentDocumentId '
            'FROM ContentDocumentLink '
            "WHERE LinkedEntity.Type = 'User' "
            'AND ContentDocument.ContentModifiedDate < {date_lt} '
            'AND ContentDocument.ContentModifiedDate > {date_gt}'
        ).format(
            date_lt=modified_date_lt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            date_gt=modified_date_gt.strftime('%Y-%m-%dT%H:%M:%SZ')
        )
    ),
    (
        {
            'modified_date_lt': modified_date_lt,
            'modified_date_gt': modified_date_gt,
            'dir_name_field': dir_name_field
        },
        (
            'SELECT LinkedEntityId, ContentDocumentId, {dir_name_field} '
            'FROM ContentDocumentLink '
            "WHERE LinkedEntity.Type = 'User' "
            'AND ContentDocument.ContentModifiedDate < {date_lt} '
            'AND ContentDocument.ContentModifiedDate > {date_gt}'
        ).format(
            date_lt=modified_date_lt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            date_gt=modified_date_gt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            dir_name_field=dir_name_field
        )
    ),
]


@pytest.mark.parametrize('args,expected_query', query_test_params)
def test_salesforce_download_content_document_link_list_queries(args, expected_query):
    client = Mock()
    client.bulk2 = Mock()
    document_link_list = Mock()
    with tempfile.TemporaryDirectory() as tmpdirname:
        salesforce = Salesforce(data_dir=tmpdirname, client=client, max_api_usage_percent=50)
        salesforce.download_content_document_link_list(
            document_link_list=document_link_list,
            obj_type='User',
            **args
        )
        client.bulk2.assert_called_with(query=expected_query, path=os.path.join(tmpdirname, 'tmp'), max_records=50000)


doc_link_csv_data = [
    # no files
    [],
    # no results from query (file with only header)
    [
        [
            ['LinkedEntityId', 'ContentDocumentId']
        ],
    ],
    # results without custom field for dir name
    [
        [
            ['LinkedEntityId', 'ContentDocumentId'],
            ['LinkedEntityId_1', 'ContentDocumentId_1'],
            ['LinkedEntityId_2', 'ContentDocumentId_2'],
        ]
    ],
    # results with custom field for dir name
    [
        [
            ['LinkedEntityId', 'ContentDocumentId', 'CustomFieldForDirName'],
            ['LinkedEntityId_1', 'ContentDocumentId_1', 'CustomFieldForDirName_1'],
            ['LinkedEntityId_2', 'ContentDocumentId_2', 'CustomFieldForDirName_2'],
        ]
    ],
    # results with custom field for dir name in multiple csv files
    [
        [
            ['LinkedEntityId', 'ContentDocumentId', 'CustomFieldForDirName'],
            ['LinkedEntityId_1', 'ContentDocumentId_1', 'CustomFieldForDirName_1'],
            ['LinkedEntityId_2', 'ContentDocumentId_2', 'CustomFieldForDirName_2'],
        ],
        [
            ['LinkedEntityId', 'ContentDocumentId', 'CustomFieldForDirName'],
            ['LinkedEntityId_3', 'ContentDocumentId_3', 'CustomFieldForDirName_3'],
            ['LinkedEntityId_4', 'ContentDocumentId_4', 'CustomFieldForDirName_4'],
        ]
    ],
]


def gen_csv(data: list[list[str]], dir_name: str):
    for file_data in data:
        temp_file_path = tempfile.mkstemp(suffix='.csv', dir=dir_name, text=True)[1]
        with open(temp_file_path, newline='', mode='w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in file_data:
                writer.writerow(row)


@pytest.mark.parametrize('data', doc_link_csv_data)
def test_salesforce_download_content_document_link_list_csv_reading(data: list[list[str]]):
    with tempfile.TemporaryDirectory() as tmpdirname:
        client = Client(sf_client=Mock())
        client.bulk2 = Mock(side_effect=lambda *args, **kwargs: gen_csv(data=data, dir_name=os.path.join(tmpdirname, 'tmp')))
        document_link_list = Mock()
        add_link_calls = []
        for file_data in data:
            for row in file_data[1:]:
                doc_link = ContentDocumentLink(linked_entity_id=row[0], content_document_id=row[1], download_dir_name=row[2] if len(row) > 2 else row[0])
                add_link_calls.append(call(doc_link))

        salesforce = Salesforce(data_dir=tmpdirname, client=client, max_api_usage_percent=50)
        salesforce.download_content_document_link_list(
            document_link_list=document_link_list,
            obj_type='User',
            dir_name_field=data[0][0][2] if len(data) and len(data[0][0]) > 2 else None
        )
        document_link_list.add_link.assert_has_calls(add_link_calls, any_order=True)

