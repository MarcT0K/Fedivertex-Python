import os
import shutil
import zipfile
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import requests
from platformdirs import user_cache_dir
from tqdm import tqdm

from .exceptions import CacheError, DownloadError

_CHUNK_SIZE = 1024 * 1024

DEFAULT_CACHE_DIR = user_cache_dir(
    appname="fedivertex-dataset",
    appauthor="MarcDamie",  # optional but recommended on Windows
)

DATASET_METADATA_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/croissant/download"
LIGHT_DATASET_METADATA_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset-reduced/croissant/download"
DATASET_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/marcdamie/fediverse-graph-dataset"
)
LIGHT_DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/marcdamie/fediverse-graph-dataset-reduced"


class CacheStatus(Enum):
    CORRUPTED = -2
    ABSENT = -1
    OUTDATED = 0
    UPTODATE = 1


def read_last_update(filepath):
    """Read the last update timestamp from a cache file.

    :param filepath: Path to the file containing the last update timestamp.
    :type filepath: Path
    :raises CacheError: if the file content is not a valid ISO datetime.
    :return: Parsed datetime of the last update.
    :rtype: datetime
    """

    try:
        with open(filepath, "r", encoding="utf-8") as update_file:
            return datetime.fromisoformat(update_file.read())
    except ValueError:
        raise CacheError("Cache corrupted (invalid update date), download necessary.")


class DatasetInfo:
    """Container for dataset-related paths and metadata.

    This class centralizes all information required to interact with the dataset,
    including cache locations, download URLs, and last update timestamps.

    :param cache_dir: Root directory for the cache.
    :type cache_dir: Path
    :param light_dataset: Whether to use the reduced version of the dataset.
    :type light_dataset: bool
    :param cache_only: If True, only local cache is used (no network requests).
    :type cache_only: bool
    :raises CacheError: if cache_only=True and no cache is available.
    :raises DownloadError: if metadata cannot be retrieved from the remote source.
    """

    def __init__(self, cache_dir: Path, light_dataset: bool, cache_only: bool):
        self.cache_root = cache_dir
        self.light_version = light_dataset
        self.dataset_dir = cache_dir / ("reduced" if self.light_version else "full")
        metadata_url = (
            LIGHT_DATASET_METADATA_URL if self.light_version else DATASET_METADATA_URL
        )
        self.data_url = LIGHT_DATASET_URL if self.light_version else DATASET_URL

        if cache_only:
            last_update_file = self.dataset_dir / "last_update.txt"
            if last_update_file.exists():
                self.last_update = read_last_update(last_update_file)
            else:
                raise CacheError("No cache found... incompatible with cache_only=True")
        else:
            try:
                resp = requests.get(metadata_url, timeout=10)
                if resp.status_code != 200:
                    raise DownloadError(
                        f"Could not retrieve dataset metadata (Invalid status {resp.status_code})"
                    )
                metadata = resp.json()
                self.last_update = metadata["dateModified"]
            except requests.RequestException as err:
                raise DownloadError(
                    f"Could not retrieve dataset metadata ({str(err)})"
                ) from err
            except KeyError as err:
                raise DownloadError(
                    "Could not retrieve dataset metadata (Missing 'dateModified' in the metadata)"
                ) from err


def download_from_http(url: str, filepath: Path):  # Inspired from Croissant ML codebase
    """Download a file from an HTTP endpoint with progress reporting.

    The file is first written to a temporary location and then atomically
    renamed to avoid partial or corrupted downloads.

    :param url: URL of the file to download.
    :type url: str
    :param filepath: Destination path for the downloaded file.
    :type filepath: Path
    :raises requests.RequestException: if the HTTP request fails.
    :return: None
    :rtype: None
    """

    response = requests.get(
        url,
        stream=True,
        timeout=10,
    )
    response.raise_for_status()
    total = int(response.headers.get("Content-Length", 0))

    tmp_path = filepath.with_suffix(".tmp")
    with (
        tmp_path.open("wb") as file,
        tqdm(
            desc="Downloading the dataset...",
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        for data in response.iter_content(chunk_size=_CHUNK_SIZE):
            size = file.write(data)
            bar.update(size)

    tmp_path.replace(filepath)


def clear_default_cache():
    """Remove the entire default cache directory.

    This deletes all cached datasets stored in the default cache location.

    :return: None
    :rtype: None
    """

    cache_dir = Path(DEFAULT_CACHE_DIR)

    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def check_for_update(dataset_info: DatasetInfo) -> CacheStatus:
    """Check whether the local cache is up-to-date with the remote dataset.

    :param dataset_info: Dataset information object.
    :type dataset_info: DatasetInfo
    :return: Cache status indicating whether the dataset is up-to-date,
             outdated, absent, or corrupted.
    :rtype: CacheStatus
    """

    update_file_path = dataset_info.dataset_dir / "last_update.txt"

    if update_file_path.exists():
        try:
            last_local_update = read_last_update(update_file_path)
        except CacheError as err:
            print(str(err))
            return CacheStatus.CORRUPTED

        print("Cache found, checking for updates...")

        if last_local_update >= dataset_info.last_update:
            print("Cache is up-to-date, no download necessary.")
            return CacheStatus.UPTODATE
        else:
            print("Cache is outdated, download necessary.")
            return CacheStatus.OUTDATED
    else:
        print("No cache found, download necessary.")
        return CacheStatus.ABSENT


def download_dataset(dataset_info: DatasetInfo):
    """Download and extract the dataset into the cache directory.

    The dataset archive is downloaded, extracted, and normalized so that
    the dataset directory has a stable name independent of versioning.

    :param dataset_info: Dataset information object.
    :type dataset_info: DatasetInfo
    :raises requests.RequestException: if the download fails.
    :raises zipfile.BadZipFile: if the archive is invalid.
    :return: None
    :rtype: None
    """

    archive_path = dataset_info.cache_root / "archive.zip"

    download_from_http(dataset_info.data_url, archive_path)

    print("Decompressing the dataset...")
    with zipfile.ZipFile(archive_path) as zip:
        zip.extractall(dataset_info.cache_root)

        # Rename the extracted folder to have a fixed name (without version)
        roots = {Path(m).parts[0] for m in zip.namelist() if m.strip()}
        if len(roots) == 1:
            old_root = dataset_info.cache_root / next(iter(roots))
            old_root.rename(dataset_info.dataset_dir)

    archive_path.unlink()


def create_update_date_file(dataset_info: DatasetInfo):
    """Write the dataset last update timestamp to the cache.

    :param dataset_info: Dataset information object.
    :type dataset_info: DatasetInfo
    :return: None
    :rtype: None
    """

    update_file_path = dataset_info.dataset_dir / "last_update.txt"

    with open(update_file_path, "w", encoding="utf-8") as update_file:
        update_file.write(dataset_info.last_update.isoformat())


def init_cache(
    light_dataset: bool, cache_dir: Optional[Path | str] = None, cache_only=False
) -> DatasetInfo:
    """Initialize dataset cache metadata without downloading data.

    This function prepares the cache directory and returns a DatasetInfo
    object describing the dataset configuration.

    :param light_dataset: Whether to use the reduced dataset version.
    :type light_dataset: bool
    :param cache_dir: Optional custom cache directory.
    :type cache_dir: Optional[Path | str]
    :param cache_only: If True, only local cache is used (no network requests).
    :type cache_only: bool
    :return: Dataset information object.
    :rtype: DatasetInfo
    """

    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    cache_dir = Path(cache_dir)
    # Create the main cache directory if necessary
    os.makedirs(cache_dir, exist_ok=True)
    return DatasetInfo(cache_dir, light_dataset, cache_only)


def load_dataset(
    light_dataset: bool, cache_dir: Optional[Path | str] = None, cache_only=False
) -> DatasetInfo:
    """Ensure the dataset is available locally and up-to-date.

    This function checks the cache status and downloads the dataset if necessary,
    unless cache_only is set to True.

    :param light_dataset: Whether to use the reduced dataset version.
    :type light_dataset: bool
    :param cache_dir: Optional custom cache directory.
    :type cache_dir: Optional[Path | str]
    :param cache_only: If True, only local cache is used (no download allowed).
    :type cache_only: bool
    :return: Dataset information object pointing to the local dataset.
    :rtype: DatasetInfo
    """

    dataset_info = init_cache(light_dataset, cache_dir, cache_only)

    if not cache_only:
        cache_status = check_for_update(dataset_info)
        if cache_status != CacheStatus.UPTODATE:
            if dataset_info.dataset_dir.exists():
                shutil.rmtree(dataset_info.dataset_dir)

            download_dataset(dataset_info)

            create_update_date_file(dataset_info)

    return dataset_info
