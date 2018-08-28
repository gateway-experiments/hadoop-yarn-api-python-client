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

.. image:: https://travis-ci.org/toidi/hadoop-yarn-api-python-client.svg?branch=master
    :target: https://travis-ci.org/toidi/hadoop-yarn-api-python-client
    :alt: Travis CI build status

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

Changelog
=========

0.2.5 - Fixed History REST API

0.2.4 - Added compatibility with HA enabled Resource Manager

.. _python-client-for-hadoop-yarn-api.readthedocs.org: http://python-client-for-hadoop-yarn-api.readthedocs.org/en/latest/
.. _hadoop.apache.org: http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html
