import pytest
from pascrd.api.tabula_sapiens import TabulaSapiensParser


def test_basic_parser_chrome():
    try:
        parser = TabulaSapiensParser()
        parser.collect_datasets()
        assert parser.file_number == len(parser.download_links) == 30
    except ValueError:
        pytest.fail("The parser is not valid.")


def test_basic_parser_firefox():
    try:
        parser = TabulaSapiensParser(browser="firefox")
        parser.collect_datasets()
        assert parser.file_number == len(parser.download_links) == 30
    except ValueError:
        pytest.fail("The parser is not valid.")


def test_invalid_browser():
    with pytest.raises(ValueError):
        TabulaSapiensParser(browser="fake")

