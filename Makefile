.PHONY: clean clean-docs clean-test clean-pyc clean-build dist docs help
.DEFAULT_GOAL := help

SA:=source activate
ENV:=hadoop-yarn-api-python-client

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


## Setup conda environments
env: ## Make a dev environment
	-conda env create --file requirements.yml --name $(ENV)

activate: ## Activate the virtualenv (default: hadoop-yarn-api-python-client)
	@echo "$(SA) $(ENV)"

nuke: ## Make clean + remove conda env
	-conda env remove -n $(ENV) -y

## Clean different build artifacts from multiple build phases

clean: clean-build clean-pyc clean-test clean-docs ## remove all build, test, coverage and Python artifacts

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

clean-docs:
	$(MAKE) -C docs clean

lint: ## check style with flake8
	$(SA) $(ENV) && flake8 yarn-api-client itests tests

test: ## run tests quickly with the default Python
	$(SA) $(ENV) && nosetests -v tests

docs: clean-docs ## generate Sphinx HTML documentation, including API docs
	$(SA) $(ENV) && $(MAKE) -C docs html

release: dist ## package and upload a release
	twine upload dist/*

dist: clean ## builds source and wheel package
	$(SA) $(ENV) && python setup.py bdist_wheel
	$(SA) $(ENV) && python setup.py sdist
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	$(SA) $(ENV) && python setup.py install
