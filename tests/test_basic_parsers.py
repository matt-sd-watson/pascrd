import pytest
from pascrd.api.tabula_sapiens import TabulaSapiensParser, download_tabula_sapiens_dataset
from pascrd.api.human_cell_atlas import HCAParser
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
