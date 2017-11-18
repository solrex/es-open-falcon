#!/usr/bin/env python

import yaml

import esmetrics

with open('conf/es-open-falcon.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

threads = []

for es_cluster in config['es-clusters']:
    metric_thread = esmetrics.EsMetrics(config['falcon']['push_url'], es_cluster['url'], es_cluster['endpoint'])
    metric_thread.start()
    threads.append(metric_thread)

for thread in threads:
    thread.join(5)
