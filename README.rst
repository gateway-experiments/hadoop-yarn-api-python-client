=============================
hadoop-yarn-api-python-client
=============================

Python client for Hadoop® YARN API

.. image:: https://coveralls.io/repos/toidi/hadoop-yarn-api-python-client/badge.png
    :target: https://coveralls.io/r/toidi/hadoop-yarn-api-python-client
    :alt: Test coverage

.. image:: https://pypip.in/version/yarn-api-client/badge.png
    :target: https://pypi.python.org/pypi/yarn-api-client/
    :alt: Latest Version

.. image:: https://pypip.in/download/yarn-api-client/badge.png
    :target: https://pypi.python.org/pypi//yarn-api-client/
    :alt: Downloads

.. image:: https://travis-ci.org/toidi/hadoop-yarn-api-python-client.svg?branch=master
    :target: https://travis-ci.org/toidi/hadoop-yarn-api-python-client
    :alt: Travis CI build status

------------
Installation
------------

From PyPI

::

    pip install yarn-api-client


From source code

::

   git clone https://github.com/toidi/hadoop-yarn-api-python-client.git
   pushd hadoop-yarn-api-python-client
   python setup.py install
   popd

-----
Usage
-----

CLI interface
=============

::

   bin/yarn_client --help

alternative

::

   python -m yarn_api_client --help

Programmatic interface
======================

.. code-block:: python

   from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
