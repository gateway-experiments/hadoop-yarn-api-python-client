# -*- coding: utf-8 -*-
import os
import xml.etree.ElementTree as ET

CONF_DIR = '/etc/hadoop/conf'


def get_resource_manager_host_port():
    config_path = os.path.join(CONF_DIR, 'yarn-site.xml')
    prop_name = 'yarn.resourcemanager.webapp.address'
    value = parse(config_path, prop_name)
    if value is not None:
        host, _, port = value.partition(':')
        return host, port
    else:
        return None


def parse(config_path, key):
    tree = ET.parse(config_path)
    root = tree.getroot()
    # Construct list with profit values
    ph1 = [{el.tag: el.text for el in p} for p in root.findall('./property')]
    # Construct dict with property key values
    ph2 = {obj['name']: obj['value'] for obj in ph1}

    value = ph2.get(key, None)
    return value
