=============================
hadoop-yarn-api-python-client
=============================

Python client for Apache Hadoop® YARN API

.. image:: https://img.shields.io/pypi/v/yarn-api-client.svg
    :target: https://pypi.python.org/pypi/yarn-api-client/
    :alt: Latest Version

.. image:: https://travis-ci.org/toidi/hadoop-yarn-api-python-client.svg?branch=master
    :target: https://travis-ci.org/toidi/hadoop-yarn-api-python-client
    :alt: Travis CI build status

.. image:: http://readthedocs.org/projects/python-client-for-hadoop-yarn-api/badge/?version=latest
    :target: https://python-client-for-hadoop-yarn-api.readthedocs.org/en/latest/?badge=latest
    :alt: Latest documentation status

.. image:: https://coveralls.io/repos/toidi/hadoop-yarn-api-python-client/badge.png
    :target: https://coveralls.io/r/toidi/hadoop-yarn-api-python-client
    :alt: Test coverage

Package documentation: python-client-for-hadoop-yarn-api.readthedocs.org_

REST API documentation: hadoop.apache.org_

------------
Installation
------------

From PyPI

::

    pip install yarn-api-client

From Anaconda (conda forge)

::

    conda install -c conda-forge yarn-api-client

From source code

::

   git clone https://github.com/toidi/hadoop-yarn-api-python-client.git
   pushd hadoop-yarn-api-python-client
   python setup.py install
   popd


Enabling support for Kerberos/SPNEGO Security

To avoid deployment issues on a non Kerberized environment, the `requests_kerberos`
dependency is optional and needs to be explicit installed in order to enable access
to YARN console protected by Kerberos/SPNEGO.

::

   pip install requests_kerberos


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

0.3.5 Release
   - Hotfix release to fix internal signature mismatch

0.3.4 Release
   - More flexible support for discovering Hadoop configuration
     including multiple Resource Managers when HA is configured
   - Properly support YARN post response codes

0.3.3 Release
   - Properly set Content-Type in PUT requests
   - Check for HADOOP_CONF_DIR env variable

0.3.2 Release
   - Make Kerberos/SPNEGO dependency optional

0.3.1 Release
   - Fix cluster_application_kill API

0.3.0 Release
    - Add support for YARN endpoints protected by Kerberos/SPNEGO
    - Moved to `requests` package for REST API invocation
    - Remove `http_con` property, as connections are now managed by `requests` package

0.2.5 Release
    - Fixed History REST API

0.2.4 Release
    - Added compatibility with HA enabled Resource Manager

.. _python-client-for-hadoop-yarn-api.readthedocs.org: http://python-client-for-hadoop-yarn-api.readthedocs.org/en/latest/
.. _hadoop.apache.org: http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html
