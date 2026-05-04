import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from turtle import down
from typing import Optional

import requests
from platformdirs import user_cache_dir
from tqdm import tqdm

from .exceptions import DownloadError

_CHUNK_SIZE = 1024

DEFAULT_CACHE_DIR = user_cache_dir(
    appname="fedivertex-dataset",
    appauthor="MarcDamie",  # optional but recommended on Windows
)

DATASET_METADATA_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/croissant/download"
LIGHT_DATASET_METADATA_URL = "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset-reduced/croissant/download"
DATASET_URL = (
    "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset/download"
)
LIGHT_DATASET_URL = (
    "https://www.kaggle.com/datasets/marcdamie/fediverse-graph-dataset-reduced/download"
)


def download_from_http(url: str, filepath: Path): # Inspired from Croissant ML codebase
    response = requests.get(
        url,
        stream=True,
        timeout=10,
    )
    response.raise_for_status()
    total = int(response.headers.get("Content-Length", 0))
    with (
        filepath.open("wb") as file,
        tqdm(
            desc=f"Downloading {url}...",
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar,
    ):
        for data in response.iter_content(chunk_size=_CHUNK_SIZE):
            size = file.write(data)
            bar.update(size)

def clear_cache(cache_dir=Path(DEFAULT_CACHE_DIR)):
    if os.path.exists(cache_dir):
        os.rmdir(cache_dir)

def check_for_update(light_dataset, cache_dir):
    if light_dataset:
        dataset_update_file = "last_update_reduced.txt"
        metadata_url = LIGHT_DATASET_METADATA_URL
    else:
        dataset_update_file = "last_update_full.txt"
        metadata_url = DATASET_METADATA_URL

    update_file_path = cache_dir / dataset_update_file

    if os.path.exists(update_file_path):
        print("Cache found, checking for updates...")
        with open(update_file_path, "r", encoding="utf-8") as update_file:
            last_local_update = datetime.fromisoformat(update_file.read())

        try:
            resp = requests.get(metadata_url)
            if resp.status_code != 200:
                raise DownloadError(
                    f"Could not retrieve dataset metadata (Invalid status {resp.status_code})"
                )
            metadata = resp.json()
            last_online_update = datetime.fromisoformat(metadata["dateModified"]).replace(tzinfo=timezone.utc)
        except requests.RequestException as err:
            raise DownloadError(
                f"Could not retrieve dataset metadata ({str(err)})"
            ) from err
        except KeyError as err:
            raise DownloadError(
                f"Could not retrieve dataset metadata (Missing 'dateModified' in the metadata)"
            ) from err

        if last_local_update > last_online_update:
            print("Cache is up-to-date, no download necessary.")
            return False
        else
            print("Cache is outdated, download necessary.")
            return True
    else:
        print("No cache found, download necessary.")
        return True

def download_dataset(light_dataset, cache_dir):
    if light_dataset:
        data_url = LIGHT_DATASET_URL
        dataset_dir = "reduced"
    else:
        data_url = DATASET_URL
        dataset_dir = "full"

    archive_path = cache_dir / "archive.zip"
    dataset_path = cache_dir / dataset_dir

    download_from_http(data_url, archive_path)


    print("Decompressing the dataset...")
    with zipfile.ZipFile(archive_path) as zip:
        zip.extractall(dataset_path)

    os.remove(archive_path)

def create_update_date_file(light_dataset, cache_dir):
    if light_dataset:
        dataset_update_file = "last_update_reduced.txt"
    else:
        dataset_update_file = "last_update_full.txt"

    update_file_path = cache_dir / dataset_update_file

    with open(update_file_path, "w", encoding="utf-8") as update_file:
        date_now = datetime.now(timezone.utc).isoformat()
        update_file.write(date_now)

def init_cache(light_dataset: bool, cache_dir: Optional[Path | str] = None) -> Path:
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR

    cache_dir = Path(cache_dir)

    if check_for_update(cache_dir=cache_dir,light_dataset=light_dataset):
        clear_cache(cache_dir) # Remove existing cache files as outdated

        os.makedirs(cache_dir) # Recreate the cache

        download_dataset(cache_dir=cache_dir,light_dataset=light_dataset)

        create_update_date_file(cache_dir=cache_dir,light_dataset=light_dataset)

    return cache_dir
