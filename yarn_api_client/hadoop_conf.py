# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET
try:
    from httplib import HTTPConnection, OK
except ImportError:
    from http.client import HTTPConnection, OK
CONF_DIR = os.getenv('HADOOP_CONF_DIR', '/etc/hadoop/conf')


def _get_rm_ids(hadoop_conf_path):
    rm_ids = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.resourcemanager.ha.rm-ids')
    if rm_ids is not None:
        rm_ids = rm_ids.split(',')
    return rm_ids

def _get_maximum_container_memory(hadoop_conf_path):
    container_memory = int(parse(os.path.join(hadoop_conf_path,'yarn-site.xml'), 'yarn.nodemanager.resource.memory-mb'))
    return container_memory

def _get_resource_manager(hadoop_conf_path, rm_id=None):
    prop_name = 'yarn.resourcemanager.webapp.address'
    if rm_id:
        rm_webapp_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), '%s.%s' % (prop_name, rm_id))
    else:
        rm_webapp_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), prop_name)

    return rm_webapp_address or None


def check_is_active_rm(url, timeout=30):
    conn = HTTPConnection(host=url, timeout=timeout)
    try:
        conn.request('GET', '/cluster')
    except:
        return False
    response = conn.getresponse()
    if response.status != OK:
        return False
    else:
        if response.getheader('Refresh', None) is not None:
            return False
    return True


def get_resource_manager_endpoint(timeout=30):
    hadoop_conf_path = CONF_DIR
    rm_ids = _get_rm_ids(hadoop_conf_path)
    if rm_ids:
        for rm_id in rm_ids:
            ret = _get_resource_manager(hadoop_conf_path, rm_id)
            if ret:
                if check_is_active_rm(ret, timeout):
                    return ret
        return None
    else:
        return _get_resource_manager(hadoop_conf_path, None)


def get_jobhistory_endpoint():
    config_path = os.path.join(CONF_DIR, 'mapred-site.xml')
    prop_name = 'mapreduce.jobhistory.webapp.address'
    return parse(config_path, prop_name)


def get_nodemanager_endpoint():
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    prop_name = 'yarn.nodemanager.webapp.address'
    return parse(config_path, prop_name)


def get_webproxy_endpoint(timeout=30):
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    prop_name = 'yarn.web-proxy.address'
    value = parse(config_path, prop_name)
    return value or get_resource_manager_endpoint(timeout)


def parse(config_path, key):
    tree = ET.parse(config_path)
    root = tree.getroot()
    # Construct list with profit values
    ph1 = [dict((el.tag, el.text) for el in p) for p in root.findall('./property')]
    # Construct dict with property key values
    ph2 = dict((obj['name'], obj['value']) for obj in ph1)

    value = ph2.get(key, None)
    return value
