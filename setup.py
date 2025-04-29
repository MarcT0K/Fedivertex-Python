from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="fedivertex",
    version="0.9.7",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Interface to download and interact with Fedivertex, the Fediverse Graph Dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    license="GPLv3",
    python_requires=">=3.10",  # To be compatible with mlcroissant
    install_requires=[
        "numpy<2.0",  # To be compatible with mlcroissant
        "mlcroissant",
        "networkx",
        "tqdm",
    ],
    extras_require={"test": ["pytest", "pytest-coverage"]},
)
