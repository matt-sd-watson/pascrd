from tqdm import tqdm
import urllib.request
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import logging
import json


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
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()
        if os.path.isfile(str(os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                                   'data', 'hca.json')))):
            with open(str(os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                                   'data', 'hca.json')))) as metadata_json:
                self.project_metadata = json.load(metadata_json)

        self.search_results = None

    def collect_project_identifiers(self):
        with urllib.request.urlopen(self.directory) as project_url:
            data = json.load(project_url)
            json_dict = data['termFacets']['project']
            for elem in json_dict['terms']:
                self.project_identifiers[elem['term']] = elem['projectId']

    def get_json_metadata(self, project, catalog="dcp23"):
        url = f'{self.directory}/{project}' if not self.directory.endswith('/') else f'{self.directory}{project}'
        return self.session.get(url, params={'catalog': catalog}).json()

    def collect_project_metadata(self, verbose=True, write_local=True):
        self.project_metadata = {}
        count = 0
        for project_key, project_value in self.project_identifiers.items():
            count += 1
            project_value = project_value[0] if isinstance(project_value, list) else project_value
            if verbose:
                self.logger.info(f"Processing dataset {count} of {len(self.project_identifiers)}: "
                                 f"{project_key} = {project_value}")
            self.project_metadata[project_value] = self.get_json_metadata(project_value)

        if write_local:
            with open(str(os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                                       'data', 'hca.json'))), 'w') as metadata_json:
                json.dump(self.project_metadata, metadata_json)

    def search(self, search_dict=None, search_type="union"):
        if search_type not in ["intersection", "union"]:
            raise ValueError("The search type must be either of intersection or union.")
        search_results = []
        for key, value in search_dict.items():
            one_search_results = []
            for project_key, project_values in self.project_metadata.items():
                for searched_elem in search_through_metadata_for_value(project_values, key=key, value=value,
                                                                       project_key=project_key):
                    if searched_elem not in one_search_results:
                        one_search_results.append(searched_elem)
            search_results.append(one_search_results)
        if search_type == "union":
            found = []
            for sub_list in search_results:
                for elem in sub_list:
                    if elem not in found:
                        found.append(elem)
            return found
        elif search_type == "intersection":
            counts = {}
            for sub_list in search_results:
                for elem in sub_list:
                    if elem not in counts.keys():
                        counts[elem] = 1
                    else:
                        counts[elem] += 1
            return list({k: v for (k, v) in counts.items() if v == len(search_dict)}.keys())


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


def search_through_metadata_for_value(tree, current_key=None, key=None, value=None, project_key=None):
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
