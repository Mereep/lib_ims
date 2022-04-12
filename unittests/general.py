import asyncio
import logging
import uuid
from pathlib import Path
from unittest import TestCase
import requests

from lib_ims import get_ims_versions, download_db
from lib_ims.utils import IMS_DATASET_META, download_db

import tempfile

from lib_ims.utils import get_ims_logger


class GeneralTests(TestCase):
    def setUp(self) -> None:
        self.used_dev_test_id = '_dev_test_small'
        ims_logger: logging.Logger = get_ims_logger()
        ims_logger.setLevel(logging.DEBUG)
        ims_logger.addHandler(logging.StreamHandler())

    def test_db_database_format_correct(self):
        get_ims_versions()  # will raise on error

    def test_db_at_least_one_database_exists(self):
        self.assertGreaterEqual(len(get_ims_versions()), 1)

    def test_links_are_available(self):
        for item, meta in IMS_DATASET_META.items():
            heads = requests.head(meta['url'])
            self.assertEqual(200, heads.status_code, f"url for dataset `{item}` not available (Code: 923482930)")

    def test_db_urls_return_correct_content_type(self):
        """ links are expected to return a zip datastream"""

        for item, meta in IMS_DATASET_META.items():
            heads = requests.head(meta['url'])
            self.assertEqual('application/zip', heads.headers.get('Content-Type'), f"`{meta['url']}` "
                                                                                   f"should report a zip file")

    def test_db_requesting_invalid_dataset_should_raise(self):
        with self.assertRaises(AssertionError):
            download_db(version='some random stufffffieiieieiiei dens nicht gibt')

    def test_db_download_testzip_should_work(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            download_db(target_path=path, version=self.used_dev_test_id)
            files: list[Path] = [*path.iterdir()]

            has_licence_file = False
            has_csv_file = False
            has_json_file = False

            for file in files:
                print(file.name)
                if file.name.endswith('.csv'):
                    has_csv_file = True
                if file.name.endswith('.json'):
                    has_json_file = True
                if file.name.endswith('licence.txt'):
                    has_licence_file = True

        self.assertTrue(has_licence_file and has_json_file and has_csv_file)
