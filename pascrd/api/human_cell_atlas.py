import requests
from tqdm import tqdm
import urllib.request
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urllib.request import urlopen, urlretrieve
import threading
from queue import Queue
import requests
import time
import logging
import json
from collections import defaultdict
from operator import ge, le


class HCAParser:
    def __init__(self, repo_directory="https://service.azul.data.humancellatlas.org/index/projects/",
                 session_retries=3, session_backoff=0.5):
        self.directory = repo_directory
        self.session = requests.Session()
        session_retry = Retry(connect=session_retries, backoff_factor=session_backoff)
        session_adapter = HTTPAdapter(max_retries=session_retry)
        self.session.mount('http://', session_adapter)
        self.session.mount('https://', session_adapter)
        self.project_identifiers = {}
        self.project_metadata = {}
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

    def collect_project_identifiers(self):
        with urllib.request.urlopen(self.directory) as project_url:
            data = json.load(project_url)
            json_dict = data['termFacets']['project']
            for elem in json_dict['terms']:
                self.project_identifiers[elem['term']] = elem['projectId']

    def get_json_metadata(self, project, catalog="dcp22"):
        url = f'{self.directory}/{project}' if not self.directory.endswith('/') else f'{self.directory}{project}'
        return self.session.get(url, params={'catalog': catalog}).json()

    def collect_project_metadata(self, verbose=True):
        count = 0
        for project_key, project_value in self.project_identifiers.items():
            count += 1
            project_value = project_value[0] if isinstance(project_value, list) else project_value
            if verbose:
                self.logger.info(f"Processing dataset {count} of {len(self.project_identifiers)}: "
                                 f"{project_key} = {project_value}")
            self.project_metadata[project_value] = self.get_json_metadata(project_value)


def print_all_nested(tree, tree_key=None, project_id=None):

    # case 1: if it is still a dictionary, recursively go through all entries
    if isinstance(tree, dict):
        for key, value in tree.items():
            print_all_nested(value, key, project_id)
    # case 2: if it is a list, evaluate the elem
    elif isinstance(tree, list):
        # case 2.1: if any of the elems are a dictionary, recursively go through them
        if any(isinstance(element, dict) for element in tree):
            for element in tree:
                if isinstance(element, dict):
                    for key, value in element.items():
                        print_all_nested(value, key, project_id)
                elif isinstance(element, list):
                    for sub_elem in element:
                        print_all_nested(sub_elem, element, project_id)
                else:
                    print_all_nested(element, tree_key, project_id)
        else:
            for element in tree:
                print(f"found {element} at {tree_key} in {project_id}")
    else:
        print(f"found {tree} at {tree_key} in {project_id}")


def download_file(url, output_path):
    url = url.replace('/fetch', '')  # Work around https://github.com/DataBiosphere/azul/issues/2908

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url, stream=True)
    response.raise_for_status()

    total = int(response.headers.get('content-length', 0))
    print(f'Downloading to: {output_path}', flush=True)

    with open(output_path, 'wb') as f:
        with tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                size = f.write(chunk)
                bar.update(size)


def search_through_metadata_for_value(tree, current_key = None, key=None, value=None, project_key = None):
    if tree == value and key == current_key:
        yield project_key
        pass
    elif isinstance(tree, list) and value in tree and key == current_key:
        yield project_key
        pass
    # case 1: if it is still a dictionary, recursively go through all entries
    elif isinstance(tree, dict):
        for sub_key, sub_value in tree.items():
            yield from search_through_metadata_for_value(sub_value, sub_key, key, value, project_key)
    # case 2: if it is a list, evaluate the elem
    elif isinstance(tree, list):
        # case 2.1: if any of the elems are a dictionary, recursively go through them
        for element in tree:
            yield from search_through_metadata_for_value(element, current_key, key, value, project_key)


def iterate_matrices_tree(tree, keys=()):
    if isinstance(tree, dict):
        for k, v in tree.items():
            yield from iterate_matrices_tree(v, keys=(*keys, k))
    elif isinstance(tree, list):
        for file in tree:
            yield keys, file
    else:
        assert False


def bulk_download_files(endpoint_url, save_location, catalog='dcp22'):
    if not os.path.exists(save_location):
        os.mkdir(save_location)
    response = requests.get(endpoint_url, params={'catalog': catalog})
    response.raise_for_status()

    response_json = response.json()
    project = response_json['projects'][0]

    file_urls = set()
    for key in ('matrices', 'contributedAnalyses'):
        tree = project[key]
        for path, file_info in iterate_matrices_tree(tree):
            url = file_info['url']
            if url not in file_urls:
                dest_path = os.path.join(save_location, file_info['name'])
                print(file_info['name'])
                if ".h5ad" in file_info['name'] and not os.path.isfile(dest_path):
                    download_file(url, dest_path)
                    file_urls.add(url)
    print('Downloads Complete.')