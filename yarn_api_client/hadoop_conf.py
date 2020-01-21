# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET
import requests

from .base import get_logger

log = get_logger(__name__)

CONF_DIR = os.getenv('HADOOP_CONF_DIR', '/etc/hadoop/conf')


def _get_rm_ids(hadoop_conf_path):
    rm_ids = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.resourcemanager.ha.rm-ids')
    if rm_ids is not None:
        rm_ids = rm_ids.split(',')
    return rm_ids


def _get_maximum_container_memory(hadoop_conf_path):
    container_memory = int(parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'),
                                 'yarn.nodemanager.resource.memory-mb'))
    return container_memory


def _is_https_only():
    # determine if HTTPS_ONLY is the configured policy, else use http
    hadoop_conf_path = CONF_DIR
    http_policy = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.http.policy')
    if http_policy == 'HTTPS_ONLY':
        return True
    return False


def _get_resource_manager(hadoop_conf_path, rm_id=None):
    # compose property name based on policy (and rm_id)
    is_https_only = _is_https_only()

    if is_https_only:
        prop_name = 'yarn.resourcemanager.webapp.https.address'
    else:
        prop_name = 'yarn.resourcemanager.webapp.address'

    # Adjust prop_name if rm_id is set
    if rm_id:
        prop_name = "{name}.{rm_id}".format(name=prop_name, rm_id=rm_id)

    rm_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), prop_name)

    return ('https://' if is_https_only else 'http://') + rm_address if rm_address else None


def check_is_active_rm(url, timeout=30, auth=None, verify=True):
    try:
        response = requests.get(url + "/cluster", timeout=timeout, auth=auth, verify=verify)
    except requests.RequestException as e:
        log.warning("Exception encountered accessing RM '{url}': '{err}', continuing...".format(url=url, err=e))
        return False

    if response.status_code != 200:
        log.warning("Failed to access RM '{url}' - HTTP Code '{status}', continuing...".format(url=url, status=response.status_code))
        return False
    else:
        return True


def get_resource_manager_endpoint(timeout=30, auth=None, verify=True):
    log.info('Getting resource manager endpoint from config: {config_path}'.format(config_path=os.path.join(CONF_DIR, 'yarn-site.xml')))
    hadoop_conf_path = CONF_DIR
    rm_ids = _get_rm_ids(hadoop_conf_path)
    if rm_ids:
        for rm_id in rm_ids:
            ret = _get_resource_manager(hadoop_conf_path, rm_id)
            if ret:
                if check_is_active_rm(ret, timeout, auth, verify):
                    return ret
        return None
    else:
        return _get_resource_manager(hadoop_conf_path, None)


def get_jobhistory_endpoint():
    config_path = os.path.join(CONF_DIR, 'mapred-site.xml')
    log.info('Getting jobhistory endpoint from config: {config_path}'.format(config_path=config_path))
    prop_name = 'mapreduce.jobhistory.webapp.address'
    return parse(config_path, prop_name)


def get_nodemanager_endpoint():
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    log.info('Getting nodemanager endpoint from config: {config_path}'.format(config_path=config_path))
    prop_name = 'yarn.nodemanager.webapp.address'
    return parse(config_path, prop_name)


def get_webproxy_endpoint(timeout=30, auth=None, verify=True):
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    log.info('Getting webproxy endpoint from config: {config_path}'.format(config_path=config_path))
    prop_name = 'yarn.web-proxy.address'
    value = parse(config_path, prop_name)
    return value or get_resource_manager_endpoint(timeout, auth, verify)


def parse(config_path, key):
    tree = ET.parse(config_path)
    root = tree.getroot()
    # Construct list with profit values
    ph1 = [dict((el.tag, el.text) for el in p) for p in root.findall('./property')]
    # Construct dict with property key values
    ph2 = dict((obj['name'], obj['value']) for obj in ph1)

    value = ph2.get(key, None)
    return value
