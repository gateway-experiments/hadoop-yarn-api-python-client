# hadoop-yarn-api-python-client

Python client for Apache Hadoop® YARN API

[![Latest Version](https://img.shields.io/pypi/v/yarn-api-client.svg)](https://pypi.python.org/pypi/yarn-api-client/)
[![Downloads](https://pepy.tech/badge/yarn-api-client/month)](https://pepy.tech/project/yarn-api-client/month)
[![Travis CI build status](https://travis-ci.org/CODAIT/hadoop-yarn-api-python-client.svg?branch=master)](https://travis-ci.org/CODAIT/hadoop-yarn-api-python-client)
[![Latest documentation status](https://readthedocs.org/projects/yarn-api-client-python/badge/?version=latest)](https://yarn-api-client-python.readthedocs.org/en/latest/?badge=latest)
[![Test coverage](https://coveralls.io/repos/toidi/hadoop-yarn-api-python-client/badge.png)](https://coveralls.io/r/toidi/hadoop-yarn-api-python-client)

Package documentation:
[yarn-api-client-python.readthedocs.org](https://yarn-api-client-python.readthedocs.org/en/latest/)

REST API documentation: [hadoop.apache.org](http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html)

**Warning**: CLI is outdated & broken. Please don't use CLI. This will be resolved in future releases.

---
## Compatibility Matrix

| yarn-api-client-python | Apache Hadoop |
| ------------- | ------------- |
| 1.0.2  | 3.2.1  |
| 1.0.3  | 3.3.0, 3.3.1  |

If u have version other than mentioned (or vendored variant like Hortonworks), certain APIs might be not working or have differences in
implementation. If u plan to use certain API long-term, you might want to make sure its not in Alpha stage in documentation.

## Installation

From PyPI
```
pip install yarn-api-client
```

From Anaconda (conda forge)
```
conda install -c conda-forge yarn-api-client
```

From source code
```
pip install git+https://github.com/CODAIT/hadoop-yarn-api-python-client.git
```

## Enabling support for SimpleAuth

See example below:
```
from yarn_api_client.auth import SimpleAuth
from yarn_api_client.history_server import HistoryServer
auth = SimpleAuth('impersonated_account_name')
history_server = HistoryServer('https://127.0.0.2:5678', auth=auth)
```

## Enabling support for Kerberos/SPNEGO Security
1. First option - using `requests_kerberos` package  

To avoid deployment issues on a non Kerberized environment, the `requests_kerberos`
dependency is optional and needs to be explicit installed in order to enable access
to YARN console protected by Kerberos/SPNEGO.

`pip install requests_kerberos`

From python code
```
from yarn_api_client.history_server import HistoryServer
from requests_kerberos import HTTPKerberosAuth
history_server = HistoryServer('https://127.0.0.2:5678', auth=HTTPKerberosAuth())
```

PS: You **need** to get valid kerberos ticket in systemwide kerberos cache before running your code, otherwise calls to kerberized environment won't go through (run kinit before proceeding to run code)

2. Second option - using `gssapi` package  

If you want to avoid using terminal calls, you have to perform SPNEGO handshake to retrieve ticket yourself. Full API documentation: https://pythongssapi.github.io/python-gssapi/latest/

# Usage

### CLI interface

**Warning**: CLI is outdated & broken. Please don't use CLI. This will be resolved in future releases.

1. First way
```
bin/yarn_client --help
```

2. Alternative way
```
python -m yarn_api_client --help
```

### Programmatic interface

```
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
am = ApplicationMaster('https://127.0.0.2:5678')
app_information = am.application_information('application_id')
```

### Changelog

1.0.3 Release
   - Drop support of Python 2.7 (if you still need it for extreme emergency, look into reverting ab4f71582f8c69e908db93905485ba4d00562dfd)
   - Update of supported hadoop version to 3.3.1
   - Add support for YARN_CONF_DIR and HADOOP_CONF_DIR
   - Add class for native SimpleAuth (#106)
   - Add constructor argument for proxies (#109)

1.0.2 Release
   - Add support for Python 3.8.x
   - Fix HTTPS url parsing
   - Fix JSON body request APIs
   - Handle YARN response with empty contents
   - Better logging support

1.0.1 Release
   - Passes the authorization instance to the Active RM check
   - Establishes a new (working) documentation site in readthedocs.io: yarn-api-client-python.readthedocs.io
   - Adds more python version (3.7 and 3.8) to test matrix and removes 2.6.

1.0.0 Release
   - Major cleanup of API.  
     - Address/port parameters have been replaced with complete
       endpoints (includes scheme [e.g., http or https]).
     - ResourceManager has been updated to take a list of endpoints for
       improved HA support.
     - ResourceManager, ApplicationMaster, HistoryServer and NodeManager
       have been updated with methods corresponding to the latest REST API.
   - pytest support on Windows has been provided.
   - Documentation has been updated.

   **NOTE:** Applications using APIs relative to releases prior to 1.0 should
   pin their dependency on yarn-api-client to _less than_ 1.0 and are encouraged
   to update to 1.0 as soon as possible.

0.3.7 Release  
   - Honor configured HTTP Policy when no address is provided - enabling
     using of HTTPS in these cases.

0.3.6 Release  
   - Extend ResourceManager to allow applications to determine
     resource availability prior to submission.

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

### Team

YARN API client is developed by an open community, and the current maintainers 
are listed below in alphabetical order:

- [Dmitry Romanenko](https://github.com/dimon222)
- [Eduard Iskandarov](https://github.com/toidi)
- [Kevin Bates](https://github.com/kevin-bates)
- [Luciano Resende](https://github.com/lresende)

