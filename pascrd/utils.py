
from tqdm import tqdm
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def search_through_hca_metadata_for_value(tree, current_key=None, key=None, value=None, project_key=None,
                                          search_type="full"):
    if search_type not in ["full", "partial"]:
        raise ValueError("The argument search_type must be either of partial or full.")
    if search_type == "full":
        if isinstance(tree, list):
            search_condition_list = value in filter(None, tree)
        elif not isinstance(tree, list) and not isinstance(tree, dict):
            search_condition_top = tree == value
    elif search_type == "partial":
        if isinstance(tree, list):
            search_condition_list = any(value.lower() in s for s in filter(None, tree) if isinstance(s, str)) or \
                                    any(value.capitalize() in s for s in filter(None, tree) if isinstance(s, str)) or \
                                    any(value.upper() in s for s in filter(None, tree) if isinstance(s, str))
        elif not isinstance(tree, list) and not isinstance(tree, dict):
            search_condition_top = tree is not None and tree is not False and isinstance(tree, str) and \
                                   (value.lower() in tree or value.capitalize() in tree or value.upper() in tree)

    if not isinstance(tree, list) and not isinstance(tree, dict) and key == current_key and search_condition_top:
        yield project_key
        pass
    elif isinstance(tree, list) and key == current_key and search_condition_list:
        yield project_key
        pass
    # case 1: if it is still a dictionary, recursively go through all entries
    elif isinstance(tree, dict):
        for sub_key, sub_value in tree.items():
            yield from search_through_hca_metadata_for_value(sub_value, sub_key, key, value, project_key, search_type)
    # case 2: if it is a list, evaluate the elem
    elif isinstance(tree, list):
        # case 2.1: if any of the elems are a dictionary, recursively go through them
        for element in tree:
            yield from search_through_hca_metadata_for_value(element, current_key, key, value, project_key, search_type)


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


def collect_unique_hca_metadata_fields(tree, tree_key=None):

    # case 1: if it is still a dictionary, recursively go through all entries
    if isinstance(tree, dict):
        for key, value in tree.items():
            yield from collect_unique_hca_metadata_fields(value, key)
    # case 2: if it is a list, evaluate the elem
    elif isinstance(tree, list):
        # case 2.1: if any of the elems are a dictionary, recursively go through them
        if any(isinstance(element, dict) for element in tree):
            for element in tree:
                if isinstance(element, dict):
                    for key, value in element.items():
                        yield from collect_unique_hca_metadata_fields(value, key)
                elif isinstance(element, list):
                    for sub_elem in element:
                        yield from collect_unique_hca_metadata_fields(sub_elem, element)
                else:
                    yield from collect_unique_hca_metadata_fields(element, tree_key)
        else:
            for element in tree:
                yield {tree_key: element}
    else:
        yield {tree_key: tree}


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
