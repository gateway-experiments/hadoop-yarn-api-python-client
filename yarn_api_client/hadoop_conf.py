# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET
try:
    from httplib import HTTPConnection, OK
except ImportError:
    from http.client import HTTPConnection, OK

CONF_DIR = '/etc/hadoop/conf'


def _get_rm_ids(hadoop_conf_path):
    rm_ids = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), 'yarn.resourcemanager.ha.rm-ids')
    if rm_ids is not None:
        rm_ids = rm_ids.split(',')
    return rm_ids


def _get_resource_manager(hadoop_conf_path, rm_id = None):
    prop_name = 'yarn.resourcemanager.webapp.address'
    if rm_id is not None:
        rm_webapp_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), '%s.%s' % (prop_name, rm_id))
    else:
        rm_webapp_address = parse(os.path.join(hadoop_conf_path, 'yarn-site.xml'), prop_name)
    if rm_webapp_address is not None:
        [host, port] = rm_webapp_address.split(':')
        return (host, port)
    else:
        return None


def _check_is_active_rm(rm_web_host, rm_web_port):
    conn = HTTPConnection(rm_web_host, rm_web_port)
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


def get_resource_manager_host_port():
    hadoop_conf_path = CONF_DIR
    rm_ids = _get_rm_ids(hadoop_conf_path)
    if rm_ids is not None:
        for rm_id in rm_ids:
            ret = _get_resource_manager(hadoop_conf_path, rm_id)
            if ret is not None:
                (host, port) = ret
                if _check_is_active_rm(host, port):
                    return host, port
        return None
    else:
        return _get_resource_manager(hadoop_conf_path, None)


def get_jobhistory_host_port():
    config_path = os.path.join(CONF_DIR, 'mapred-site.xml')
    prop_name = 'mapreduce.jobhistory.webapp.address'
    value = parse(config_path, prop_name)
    if value is not None:
        host, _, port = value.partition(':')
        return host, port
    else:
        return None


def get_webproxy_host_port():
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    prop_name = 'yarn.web-proxy.address'
    value = parse(config_path, prop_name)
    if value is not None:
        host, _, port = value.partition(':')
        return host, port
    else:
        return get_resource_manager_host_port()


def parse(config_path, key):
    tree = ET.parse(config_path)
    root = tree.getroot()
    # Construct list with profit values
    ph1 = [dict((el.tag, el.text) for el in p) for p in root.findall('./property')]
    # Construct dict with property key values
    ph2 = dict((obj['name'], obj['value']) for obj in ph1)

    value = ph2.get(key, None)
    return value
