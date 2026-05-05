import os

from fedivertex import GraphLoader
from fedivertex.cache import clear_cache


def test_cache_status(capsys):
    clear_cache()
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

    update_file_path = loader.CACHE_DIR / "reduced" / "last_update.txt"
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
        update_file.write("2019-05-05T07:24:39.383197+00:00")

    _loader = GraphLoader()
    captured = capsys.readouterr()
    assert (
        "Cache found, checking for updates...\nCache is outdated, download necessary.\nDecompressing the dataset...\n"
        == captured.out
    )
    del _loader
