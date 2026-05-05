import os
from pathlib import Path

from fedivertex import GraphLoader
from fedivertex.cache import DEFAULT_CACHE_DIR, clear_default_cache


def test_cache_removal():
    cache_path = Path(DEFAULT_CACHE_DIR)
    assert cache_path.exists()

    clear_default_cache()

    assert not cache_path.exists()


def test_cache_status(capsys):
    clear_default_cache()
    _loader = GraphLoader()
    captured = capsys.readouterr()
    assert (
        "No cache found, download necessary.\nDecompressing the dataset...\n"
        == captured.out
    )
    del _loader

    loader = GraphLoader()
    captured = capsys.readouterr()
    assert (
        "Cache found, checking for updates...\nCache is up-to-date, no download necessary.\n"
        == captured.out
    )

    update_file_path = loader.DATASET_INFO.dataset_dir / "last_update.txt"
    os.remove(update_file_path)
    with open(update_file_path, "w") as update_file:
        update_file.write("INVALID DATA")

    del loader

    _loader = GraphLoader()
    captured = capsys.readouterr()
    assert (
        "Cache corrupted (invalid update date), download necessary.\nDecompressing the dataset...\n"
        == captured.out
    )
    del _loader

    os.remove(update_file_path)
    with open(update_file_path, "w") as update_file:
        update_file.write("2016-04-24T12:08:29.887")

    _loader = GraphLoader()
    captured = capsys.readouterr()
    assert (
        "Cache found, checking for updates...\nCache is outdated, download necessary.\nDecompressing the dataset...\n"
        == captured.out
    )
