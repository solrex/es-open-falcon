#!/usr/bin/env python

import yaml

import esmetrics

with open('conf/es-open-falcon.yml', 'r') as ymlfile:
    config = yaml.load(ymlfile)

for es_cluster in config['es-clusters']:
    es_metric_thread = esmetrics.EsMetrics(config['falcon']['push_url'], es_cluster['url'], es_cluster['endpoint'])
    es_metric_thread.start()
