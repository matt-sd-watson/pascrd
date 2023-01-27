import urllib.request
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import logging
import json
from pascrd.utils import search_through_hca_metadata_for_value, collect_unique_hca_metadata_fields


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
        self.search_options = None
        self._collect_search_options()

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

    def search(self, search_dict=None, search_type="union", match_type="full"):
        if search_type not in ["intersection", "union"]:
            raise ValueError("The argument search_type must be either of intersection or union.")
        if match_type not in ["full", "partial"]:
            raise ValueError("The argument match_type must be either of partial or full.")

        search_results = []
        for key, value in search_dict.items():
            originally_list = isinstance(value, list)
            value = [value] if not originally_list else value
            if not originally_list:
                one_search_results = []
            for sub_search in value:
                if originally_list:
                    one_search_results = []
                for project_key, project_values in self.project_metadata.items():
                    for searched_elem in search_through_hca_metadata_for_value(project_values, key=key,
                                                                               value=sub_search,
                                                                           project_key=project_key,
                                                                           search_type=match_type):
                        if searched_elem not in one_search_results:
                            one_search_results.append(searched_elem)
                    if originally_list:
                        search_results.append(one_search_results)
            if not originally_list:
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

    def _collect_search_options(self):
        self.search_options = {}
        for key, value in self.project_metadata.items():
            for project_elem in collect_unique_hca_metadata_fields(value):
                for sub_key, sub_value in project_elem.items():
                    if sub_key not in self.search_options.keys():
                        self.search_options[sub_key] = [sub_value]
                    else:
                        if sub_value not in self.search_options[sub_key]:
                            self.search_options[sub_key].append(sub_value)
        return self.search_options
