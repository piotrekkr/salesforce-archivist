import tempfile

from test.salesforce.helper import gen_csv
from unittest.mock import call, patch

import pytest

from salesforce_archivist.salesforce.content_document_link import ContentDocumentLink, ContentDocumentLinkList


@pytest.mark.parametrize(
    "entity_id, did, dir_name, expected_dir_name",
    [("EID", "DOCID", "DownloadDirName", "DownloadDirName"), ("EID", "DOCID", None, "EID")],
)
def test_content_document_link_properties(entity_id, did, dir_name, expected_dir_name):
    link = ContentDocumentLink(linked_entity_id=entity_id, content_document_id=did, download_dir_name=dir_name)
    assert link.linked_entity_id == entity_id
    assert link.content_document_id == did
    assert link.download_dir_name == expected_dir_name


def test_content_document_link_equality():
    link1 = ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name")
    link2 = ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name")
    assert link1 == link2


@patch("os.path.exists")
def test_content_document_link_list_data_file_exist(exists_mock):
    exists_mock.side_effect = [True, False]
    data_dir = "/fake/dir"
    version_list = ContentDocumentLinkList(data_dir=data_dir)
    assert version_list.data_file_exist()
    assert not version_list.data_file_exist()


@pytest.mark.parametrize(
    "csv_data",
    [
        [
            [
                ["LinkedEntityId", "ContentDocumentId", "LinkedEntity.Username"],
            ],
        ],
        [
            [
                ["LinkedEntityId", "ContentDocumentId", "LinkedEntity.Username"],
                ["Id_1", "ContentDocumentId_1", "User1"],
                ["Id_2", "ContentDocumentId_2", "User2"],
            ],
        ],
    ],
)
def test_content_document_link_list_load_data_from_file(csv_data):
    with tempfile.TemporaryDirectory() as tmpdirname:
        with patch.object(ContentDocumentLinkList, "add_link") as add_link_mock:
            link_list = ContentDocumentLinkList(data_dir=tmpdirname)
            gen_csv(data=csv_data, path=link_list.path)
            link_list.load_data_from_file()
            expected_calls = []
            for i, row in enumerate(csv_data):
                if not i:
                    continue
                expected_calls.append(
                    call(
                        version=ContentDocumentLink(
                            linked_entity_id=row[0], content_document_id=row[1], download_dir_name=row[3]
                        )
                    )
                )
            assert add_link_mock.mock_calls == expected_calls


def test_content_document_link_list_save():
    with tempfile.TemporaryDirectory() as tmpdirname:
        link_list = ContentDocumentLinkList(data_dir=tmpdirname)
        to_save = [
            ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name"),
            ContentDocumentLink(linked_entity_id="EID2", content_document_id="did2", download_dir_name="dir_name2"),
        ]
        for link in to_save:
            link_list.add_link(doc_link=link)
        link_list.save()
        loaded_list = ContentDocumentLinkList(data_dir=tmpdirname)
        loaded_list.load_data_from_file()
        loaded_links = [link for link in loaded_list]
        assert len(loaded_list) == len(to_save)
        for link in to_save:
            assert link in loaded_links


def test_content_document_link_list_add_version():
    link_list = ContentDocumentLinkList(data_dir="/fake/dir")
    link = ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name")
    link_list.add_link(doc_link=link)
    assert next(link_list.__iter__()) == link


def test_content_document_link_list_add_version_does_not_allow_duplicates():
    link_list = ContentDocumentLinkList(data_dir="/fake/dir")
    link = ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name")
    link_list.add_link(doc_link=link)
    link_list.add_link(doc_link=link)
    assert len(link_list) == 1


def test_content_document_link_list_len():
    link_list = ContentDocumentLinkList(data_dir="/fake/dir")
    link_list.add_link(
        doc_link=ContentDocumentLink(linked_entity_id="EID", content_document_id="did", download_dir_name="dir_name")
    )
    link_list.add_link(
        doc_link=ContentDocumentLink(linked_entity_id="EID2", content_document_id="did2", download_dir_name="dir_name2")
    )
    assert 2 == len(link_list)
