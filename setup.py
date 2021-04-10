import os
import setuptools


def read_file(file_name):
    path = os.path.join(os.path.dirname(__file__), file_name)
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


setuptools.setup(
    name="pandas-alchemy",
    version="0.0.1",
    author="Junyuan Chen",
    author_email="junyuan.chen.c@gmail.com",
    description="SQL based, Pandas compatible DataFrame & Series",
    license="MIT",
    url="https://github.com/JunyuanChen/pandas-alchemy",
    packages=["pandas_alchemy"],
    long_description=read_file("README.md"),
    long_description_content_type="text/markdown",
    install_requires=[
        "sqlalchemy>=1.4, <2",
        "pandas>=1.2, <2"
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Information Analysis"
    ]
)
