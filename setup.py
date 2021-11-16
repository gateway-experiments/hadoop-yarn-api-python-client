# -*- coding: utf-8 -*-
import codecs
import os
import re
from setuptools import setup, find_packages


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name = 'yarn-api-client',
    version = find_version('yarn_api_client', '__init__.py'),
    description='Python client for Hadoop® YARN API',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    packages = find_packages(exclude=['tests','itests']),

    install_requires = [
        'requests>=2.7,<3.0',
    ],

    entry_points = {
        'console_scripts': [
            'yarn_client = yarn_api_client.main:main',
        ],
    },

    tests_require = ['mock', 'flake8'],
    test_suite = 'tests',

    author = 'Iskandarov Eduard',
    author_email = 'eduard.iskandarov@ya.ru',
    maintainer = 'Dmitry Romanenko',
    maintainer_email = 'dmitry@romanenko.in',
    license = 'BSD',
    url = 'https://github.com/CODAIT/hadoop-yarn-api-python-client',
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: System :: Distributed Computing',
    ],
)
