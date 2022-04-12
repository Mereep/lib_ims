from __future__ import annotations

import asyncio
import os
import logging
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import aiohttp
import zipfile

from lib_ims.exceptions import ImsException

BASE_PATH = Path(os.path.dirname(__file__))

"""databases index. Underscored entries are excluded from public interface"""
IMS_DATASET_META = {
    'latest': {
        'description': 'Latest version of IMS Dataset',
        'url': 'https://micloud.hs-mittweida.de/index.php/s/zRe5G4nS7rBGJxq/download/dev_test_small.zip',
        'licence_notice': 'Please take not of the included licence in the licence.txt file.'
    },
    '_dev_test_small': {
        'description': 'Dev dataset (only contains a subset of data; you shouldn\'t use it',
        'url': 'https://micloud.hs-mittweida.de/index.php/s/zRe5G4nS7rBGJxq/download/dev_test_small.zip',
        'licence_notice': 'Please take not of the included licence in the licence.txt file.'
    },
}


def get_ims_logger() -> logging.Logger:
    """
    Will return the logger used for outputs of the IMS lib and utils
    :return:
    """
    return logging.getLogger('ims')


def download_db(version: str = 'latest', target_path: Path | None = None):
    """ will download the IMS data and unpack it. This will take a while
    :param version: the ims database version to be loaded. See `get_ims_versions` to see the options.
    :param target_path: if given, the data will be extracted there if not './ims_data' will be used
    By default 'latest' will be chosen
    """
    target_path = target_path or BASE_PATH / 'ims_data'
    assert target_path.is_dir(), f"Please provide a valid directory; got `{target_path!s}` (Code: 3482390)"
    assert version in IMS_DATASET_META, "Dataset version is not known. See `get_ims_versions()` for more info " \
                                        "(Code: 439282)"

    version = IMS_DATASET_META[version]
    get_ims_logger().info(f"Will download the IMS dataset to `{target_path!s}` (Code: 32842903)")

    get_ims_logger().info(f"The dataset is under separate "
                          f"licensing: `{version['licence_notice']}` (Code: 3482930)")

    loop = asyncio.get_event_loop()
    task = _async_get_url(version['url'])
    res, = loop.run_until_complete(
        asyncio.gather(task)
    )
    zip_file_data = BytesIO(res)
    if not zipfile.is_zipfile(zip_file_data):
        raise ImsException(f"Download of {version} did not return a valid zip file (Code: 9382930)")

    get_ims_logger().info(f"Extracting zip file to `{target_path!s}` (Code: 3924890234)")
    file = zipfile.ZipFile(zip_file_data)
    file.extractall(target_path)
    get_ims_logger().info(f"Extracting to `{target_path!s}` was successful (Code: 923849203)")


def get_ims_versions() -> list[tuple[str, str, str]]:
    """ will report the available dataset options.
    :return: tuples of (version identifier, short description, licence info)
    """

    return list((key, value['description'], value['licence_notice']) for key, value in IMS_DATASET_META.items()
                if not key.startswith('_'))


async def _async_get_url(url: str, chunk_size_bytes: int = 1024 * 1024) -> bytes:
    """
    reads data in chunks and reports progress
    :param url:
    :return:
    """
    data = b''
    currently_downloaded_bytes = 0
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url) as rsp:
            content_length = int(rsp.headers.get('Content-Length'))
            while read_data := await rsp.content.read(chunk_size_bytes):
                if content_length:
                    progress_str = f"of {content_length / 1024 / 1024:.2f}MB " \
                                   f"({(currently_downloaded_bytes / content_length)*100:.2f}%)"
                else:
                    progress_str = "of unknown size"

                currently_downloaded_bytes += len(read_data)
                data += read_data
                get_ims_logger().info(f"Currently Downloaded "
                                      f"{currently_downloaded_bytes / 1024 / 1024:.2f}MB {progress_str} "
                                      f"(Code: 4823474)")

    return data
