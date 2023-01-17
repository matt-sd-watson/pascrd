import pytest
from pascrd.api.tabula_sapiens import TabulaSapiensParser
from pascrd.api.human_cell_atlas import HCAParser


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
    parser.collect_project_identifiers()
    parser.collect_project_metadata()
    assert len(parser.project_identifiers) == 313
    assert len(parser.project_metadata) == 313

