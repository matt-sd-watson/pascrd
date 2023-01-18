import logging
import time
import re
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
import requests
import os
from tqdm import *
import zipfile


class TabulaSapiensParser:
    def __init__(self, url='https://figshare.com/articles/dataset/Tabula_Sapiens_release_1_0/14267219',
                 time_delay=0.1, browser="chrome"):

        if browser not in ["chrome", "firefox"]:
            raise ValueError("the browser value must be one of: chrome, firefox")

        self.url = url
        self.time_delay = time_delay
        self.browser = browser
        self.options = ChromeOptions() if self.browser == "chrome" else FirefoxOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=self.options) if self.browser == "chrome" else \
            webdriver.Firefox(options=self.options)

        self.driver.get(self.url)
        self.download_links = {}

        try:
            file_counter = self.driver.find_element(By.CLASS_NAME, "_1Xdzb").text
            assert "files" in str(file_counter)
            self.file_number = int(file_counter.split(" files")[0])
        except NoSuchElementException:
            logging.info(f"Verify that the url {url} is a valid link for Tabula Sapiens download links.")

    def _get_links(self):
        time.sleep(self.time_delay)
        try:
            links = self.driver.find_elements(by=By.TAG_NAME, value='a')
            for link in links:
                if "ndownloader" in link.get_attribute('href') and link.get_attribute('href') not in \
                        self.download_links.values():
                    parent_of_parent = link.find_element(By.XPATH, '../..')
                    for elem in parent_of_parent.find_elements(by=By.TAG_NAME, value='span'):
                        if "zip" in elem.get_attribute('title'):
                            self.download_links[re.sub(r'^.*?TS_', '',
                                                       elem.get_attribute('title')).split('.h5ad')[0].title()] = \
                                link.get_attribute('href')
        except StaleElementReferenceException:
            self.time_delay = 1.1 * self.time_delay
            logging.info(f"increasing the rendering delay: {self.time_delay}")
            self._get_links()

    def collect_datasets(self):
        while len(self.download_links) != self.file_number:
            time.sleep(self.time_delay)
            self.driver.find_element(By.XPATH, "//button[.='Next page']").click()
            self._get_links()
        assert len(self.download_links) == self.file_number


def download_tabula_sapiens_dataset(dataset_key: str, dataset_url: str, destination_path: str, chunk_size=8192,
                                    use_unzip=True):

    dest_path = os.path.join(destination_path, dataset_key + ".h5ad.zip")

    with requests.get(dataset_url, stream=True) as r:
        r.raise_for_status()
        if not os.path.isfile(dest_path):
            with open(dest_path, 'wb') as f:
                pbar = tqdm(total=int(r.headers['Content-Length']))
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        pbar.update(len(chunk))

    if use_unzip:
        with zipfile.ZipFile(dest_path, 'r') as zip_ref:
            zip_ref.extractall(destination_path)
