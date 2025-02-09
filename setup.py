# setup.py
from setuptools import setup, find_packages

setup(
    name="data_loader",
    version="1.0.0",
    author="Emmanuel Okutue",
    author_email="okutuee@yahoo.com",
    description="A package for extracting and migrating data from SQL Server tables.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/okutue/data_loader",  # Update as needed
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pyodbc",
        "tkinter"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "data-loader=data_loader.main:main"
        ]
    },
)
