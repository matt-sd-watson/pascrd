import pytest
from pascrd.api.tabula_sapiens import TabulaSapiensParser


def test_basic_parser():
    parser = TabulaSapiensParser()
    parser.collect_datasets()
    assert parser.file_number == len(parser.download_links)
    assert parser.file_number == 30


