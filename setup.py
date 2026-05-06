from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="fedivertex",
    version="1.1.1",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Interface to download and interact with Fedivertex, the Fediverse Graph Dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    license="GPLv3",
    python_requires=">=3.10",
    install_requires=[
        "networkx",
        "networkx-temporal",
        "platformdirs",
        "requests",
        "tqdm",
    ],
    extras_require={"test": ["pytest", "pytest-coverage"]},
)
