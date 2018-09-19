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
    long_description=read('README.rst'),
    packages = find_packages(exclude=['tests','itests']),

    install_requires = [
        'requests>=2.7,<3.0',
        'requests-kerberos',
    ],
    entry_points = {
        'console_scripts': [
            'yarn_client = yarn_api_client.main:main',
        ],
    },

    tests_require = ['mock'],
    test_suite = 'tests',

    author = 'Iskandarov Eduard',
    author_email = 'e.iskandarov@corp.mail.ru',
    license = 'BSD',
    url = 'https://github.com/toidi/hadoop-yarn-api-python-client',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Distributed Computing',
    ],
)
