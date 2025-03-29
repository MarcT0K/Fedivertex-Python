from setuptools import setup, find_packages

setup(
    name="fediverse-graphs",
    version="0.0.1",
    author="Marc DAMIE",
    author_email="marc.damie@inria.fr",
    description="Interface to download and interact with the Fediverse Graph Dataset",
    packages=find_packages(),
    license="GPLv3",
    python_requires=">=3.10",  # To be compatible with mlcroissant
    install_requires=[
        "numpy<2.0",  # To be compatible with mlcroissant
        "pandas",
        "mlcroissant",
        "networkx",
        "tqdm",
    ],
    extras_require={"test": ["pytest", "pytest-coverage"]},
)
