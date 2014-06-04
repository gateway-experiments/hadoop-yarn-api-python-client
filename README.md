hadoop-yarn-api-python-client
=============================

Python client for Hadoop® YARN API

Installation
------------

From PyPI

```bash
pip install yarn-api-client
```

From source code

```bash
git clone https://github.com/toidi/hadoop-yarn-api-python-client.git
pushd hadoop-yarn-api-python-client
python setup.py install
popd
```

Usage
-----

h3. CLI interface

```bash
bin/yarn_client --help
```

alternative

```bash
python -m yarn_api_client --help
```

h3. Programmatic interface

```python
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
```
