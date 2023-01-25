import pytest
from pascrd.api.tabula_sapiens import TabulaSapiensParser, download_tabula_sapiens_dataset
from pascrd.api.human_cell_atlas import HCAParser
from pascrd.utils import collect_unique_hca_metadata_fields
import requests_mock
import os
import mock
import zipfile
from zipfile import BadZipfile
import responses
from unittest import mock
import shutil


def test_basic_parser_chrome():
    try:
        parser = TabulaSapiensParser(time_delay=0.1)
        parser.collect_datasets()
        assert parser.file_number == len(parser.download_links) == 30
    except ValueError:
        pytest.fail("The parser is not valid.")


def test_basic_parser_firefox():
    try:
        parser = TabulaSapiensParser(browser="firefox", time_delay=0.5)
        parser.collect_datasets()
        assert parser.file_number == len(parser.download_links) == 30
    except ValueError:
        pytest.fail("The parser is not valid.")


def test_invalid_browser():
    with pytest.raises(ValueError):
        TabulaSapiensParser(browser="fake")


def test_basic_hca_parser():
    parser = HCAParser()
    assert len(parser.project_metadata) == 313
    print(parser.project_metadata)


def test_basic_hca_parser_reload():
    parser = HCAParser()
    parser.collect_project_identifiers()
    parser.collect_project_metadata()
    assert len(parser.project_identifiers) == len(parser.project_metadata) == 313


@pytest.fixture(scope="function")
def get_tmp_ts_file(tmp_path):
    return str(os.path.join(tmp_path))


@requests_mock.Mocker(kw="mock")
def test_mock_download_tabula_sapiens(get_tmp_ts_file, **kwargs):
    expected_headers = {'Content-Type': 'text/html', 'Content-Length': '1'}
    kwargs["mock"].get('http://test.com', headers=expected_headers)
    download_tabula_sapiens_dataset("fake_key", "http://test.com", get_tmp_ts_file, chunk_size=1, use_unzip=False)

    assert os.path.isfile(os.path.join(get_tmp_ts_file, "fake_key.h5ad.zip"))


@requests_mock.Mocker(kw="mock")
def test_mock_download_tabula_sapiens_zipped(get_tmp_ts_file, **kwargs):
    with pytest.raises(BadZipfile):
        expected_headers = {'Content-Type': 'application/zip', 'Content-Length': '1',
                        'one': 'one', 'two': 'two', 'three': 'three'}
        kwargs["mock"].get('http://test.com', headers=expected_headers)
        download_tabula_sapiens_dataset("fake_key", "http://test.com", get_tmp_ts_file, chunk_size=1, use_unzip=True)
        assert os.path.isfile(os.path.join(get_tmp_ts_file, "fake_key.h5ad.zip"))

    shutil.copy(os.path.join(os.path.dirname(__file__), 'data', 'test.zip'), os.path.join(get_tmp_ts_file,
                                                                                          "fake_key.h5ad.zip"))
    download_tabula_sapiens_dataset("fake_key", "http://test.com", get_tmp_ts_file, chunk_size=1, use_unzip=True)
    assert os.path.isfile(os.path.join(get_tmp_ts_file, "fake_key.h5ad.zip"))


def test_bad_search():
    parser = HCAParser()
    with pytest.raises(ValueError):
        parser.search(search_type="fake")


def test_basic_search():
    parser = HCAParser()
    one_search_union = parser.search({"effectiveOrgan": "blood"})
    two_search_union = parser.search({"effectiveOrgan": "blood", 'institution': 'Broad Institute'})
    assert one_search_union != two_search_union
    assert len(two_search_union) > len(one_search_union)

    breast_parser = parser.search({"effectiveOrgan": "breast", "projectId": "a004b150-1c36-4af6-9bbd-070c06dbc17d"},
                                  search_type="union")
    assert len(breast_parser) == 11

    breast_parser_2 = parser.search({"effectiveOrgan": "breast", "projectId": "a004b150-1c36-4af6-9bbd-070c06dbc17d"},
                                    search_type="intersection")
    assert len(breast_parser_2) == 1
    assert breast_parser != breast_parser_2
    assert len(breast_parser) > len(breast_parser_2)


def test_basic_collect_search_options():
    parser = HCAParser()
    assert {"organ", "laboratory", "institution", "email"}.issubset(parser.search_options.keys())
    assert "blood" in parser.search_options["organ"]
    