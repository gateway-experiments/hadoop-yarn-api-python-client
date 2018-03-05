=============================
hadoop-yarn-api-python-client
=============================

Python client for Hadoop® YARN API

.. image:: https://coveralls.io/repos/toidi/hadoop-yarn-api-python-client/badge.png
    :target: https://coveralls.io/r/toidi/hadoop-yarn-api-python-client
    :alt: Test coverage

.. image:: https://img.shields.io/pypi/v/yarn-api-client.svg
    :target: https://pypi.python.org/pypi/yarn-api-client/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/dm/yarn-api-client.svg
    :target: https://pypi.python.org/pypi//yarn-api-client/
    :alt: Downloads

.. image:: https://travis-ci.org/toidi/hadoop-yarn-api-python-client.svg?branch=master
    :target: https://travis-ci.org/toidi/hadoop-yarn-api-python-client
    :alt: Travis CI build status

.. image:: https://caniusepython3.com/project/yarn-api-client.svg
    :target: https://caniusepython3.com/project/yarn-api-client
    :alt: Python 3 port

Package documentation: python-client-for-hadoop-yarn-api.readthedocs.org_

REST API documentation: hadoop.apache.org_

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

.. _python-client-for-hadoop-yarn-api.readthedocs.org: http://python-client-for-hadoop-yarn-api.readthedocs.org/en/latest/
.. _hadoop.apache.org: http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html
