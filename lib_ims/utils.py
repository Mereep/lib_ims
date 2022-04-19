from __future__ import annotations

import asyncio
import os
import logging
import tempfile
from asyncio import Task
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import aiohttp
import zipfile

from aiohttp import ClientTimeout

from lib_ims.exceptions import ImsException

BASE_PATH = Path(os.path.dirname(__file__))

"""databases index. Underscored entries are excluded from public interface"""
IMS_DATASET_META = {
    'ims_racing_1_paper': {
        'url': 'https://micloud.hs-mittweida.de/index.php/s/CFdk5cGjFp3Mpba/download/ims_racing1_paper.zip',
        'description': "Dataset as described in the IMSRacing Publication",
        'licence_notice': 'Attribution-ShareAlike 4.0 International. Please take note of the included licence.txt'
                          ' file'
    },
    '_dev_test_small': {
        'description': 'Dev dataset (only contains a subset of data; you shouldn\'t use it',
        'url': 'https://micloud.hs-mittweida.de/index.php/s/zRe5G4nS7rBGJxq/download/dev_test_small.zip',
        'licence_notice': 'Please take note of the included licence in the licence.txt file.'
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
    :param target_path: if given, the data will be extracted there if not '../ims_data' will be used
    By default 'latest' will be chosen

    Warning: if on jupyter notebooks (or other `inside-async` environments) import `nest_asyncio` or you likely will
    be facing a runtime error

    @see https://pypi.org/project/nest-asyncio/
    """
    target_path = target_path or BASE_PATH / '../ims_data'
    assert target_path.is_dir(), f"Please provide a valid directory; got `{target_path!s}` (Code: 3482390)"
    assert version in IMS_DATASET_META, "Dataset version is not known. See `get_ims_versions()` for more info " \
                                        "(Code: 439282)"

    version = IMS_DATASET_META[version]
    get_ims_logger().info(f"Will download the IMS dataset to `{target_path!s}` (Code: 32842903)")

    get_ims_logger().info(f"The dataset is under separate "
                          f"licensing: `{version['licence_notice']}` (Code: 3482930)")

    # if we crash here make sure to import `nest_asnycio`
    # @see https://pypi.org/project/nest-asyncio/

    download_path = target_path / 'download.zip'
    with open(download_path, 'wb') as f:
        task = asyncio.create_task(_async_get_url(version['url'], file_handle=f))
        loop = asyncio.get_event_loop()
        finished, unfinished = loop.run_until_complete(
            asyncio.wait([task], timeout=None)
        )
        task: Task = [*finished][0]
        total_bytes = task.result()
        get_ims_logger().info(f"Finished downloading ({total_bytes} bytes) to {download_path!s} "
                              f"(Code: 48293048)")

    try:
        with open(download_path, 'rb') as f:
            #data = f.read()
            #print(len(data), data[:10])
            if not zipfile.is_zipfile(f):
                raise ImsException(f"Download of {version} did not return a valid zip file (Code: 9382930)")

            get_ims_logger().info(f"Extracting zip file to `{target_path!s}` (Code: 3924890234)")
            file = zipfile.ZipFile(f)
            file.extractall(target_path)
            get_ims_logger().info(f"Extracting to `{target_path!s}` was successful (Code: 923849203)")

    finally:
        if download_path.is_file():
            get_ims_logger().info(f"Trying to delete downloaded file {download_path}")
            os.remove(download_path)
            get_ims_logger().info(f"Done")


def get_ims_versions() -> list[tuple[str, str, str]]:
    """ will report the available dataset options.
    :return: tuples of (version identifier, short description, licence info)
    """

    return list((key, value['description'], value['licence_notice']) for key, value in IMS_DATASET_META.items()
                if not key.startswith('_'))


async def _async_get_url(url: str, file_handle: BinaryIO, chunk_size_bytes: int = 1024 * 1024) -> int:
    """
    reads data in chunks and reports progress

    :param url:
    :return: total bytes read
    """
    currently_downloaded_bytes = 0
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=ClientTimeout(total=6000)) as rsp:
            content_length = int(rsp.headers.get('Content-Length'))
            while read_data := await rsp.content.read(chunk_size_bytes):
                if content_length:
                    progress_str = f"of {content_length / 1024 / 1024:.2f}MB " \
                                   f"({(currently_downloaded_bytes / content_length)*100:.2f}%)"
                else:
                    progress_str = "of unknown size"

                currently_downloaded_bytes += len(read_data)
                file_handle.write(read_data)

                if (currently_downloaded_bytes % (1024 * 1024 * 5)) == 0:
                    get_ims_logger().info(f"Currently Downloaded "
                                          f"{currently_downloaded_bytes / 1024 / 1024:.2f}MB {progress_str} "
                                          f"(Code: 4823474)")

    return currently_downloaded_bytes
